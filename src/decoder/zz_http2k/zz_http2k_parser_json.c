/*
**
** Copyright (C) 2014-2016, Eneo Tecnologia S.L.
** Copyright (C) 2017, Eugenio Perez <eupm90@gmail.com>
** Copyright (C) 2018-2019, Wizzie S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
**
** This file is part of n2kafka.
** All rights reserved.
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU Affero General Public License as
** published by the Free Software Foundation, either version 3 of the
** License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU Affero General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "zz_http2k_parser_json.h"

#include "zz_http2k_parser.h"

#include "decoder/decoder_api.h"

#include "util/kafka_message_array.h"
#include "util/topic_database.h"
#include "util/util.h"

#include <librd/rdlog.h>
#include <librdkafka/rdkafka.h>
#include <yajl/yajl_parse.h>

#include <assert.h>
#include <string.h>
#include <syslog.h>

#define YAJL_PARSER_OK 1
// #define YAJL_PARSER_ABORT 0

//
// PARSING
//

static int zz_parse_start_json_map(void *ctx) {
	struct zz_session *sess = ctx;
	const size_t curr_bytes_consumed = yajl_get_bytes_consumed(
			sess->json_session.yajl_handler);
	if (0 == sess->json_session.stack_pos++) {
		sess->json_session.http_chunk.last_open_map =
				sess->json_session.http_chunk.in_buffer +
				curr_bytes_consumed - sizeof((char)'{');
	}

	return YAJL_PARSER_OK;
}

/** Generate kafka message.
 */
static void zz_parse_generate_rdkafka_message(struct zz_session *sess,
					      rd_kafka_message_t *msg) {
	assert(sess);
	assert(msg);
	const char *end_msg = sess->json_session.http_chunk.in_buffer +
			      yajl_get_bytes_consumed(
					      sess->json_session.yajl_handler);
	assert(end_msg > sess->json_session.http_chunk.last_open_map);
	*msg = (rd_kafka_message_t){
			.payload = const_cast(sess->json_session.http_chunk
							      .last_open_map),
			.len = (size_t)(end_msg -
					sess->json_session.http_chunk
							.last_open_map),
	};
}

static int zz_parse_end_json_map0(struct zz_session *sess) {
	rd_kafka_message_t msg;
	zz_parse_generate_rdkafka_message(sess, &msg);
	const int add_rc = kafka_msg_array_add(&sess->kafka_msgs, &msg);

	// This is not the last message anymore
	sess->json_session.http_chunk.last_open_map = NULL;
	if (unlikely(add_rc != 0)) {
		rdlog(LOG_ERR, "Couldn't add kafka message (OOM?)");
	}

	return YAJL_PARSER_OK;
}

/** Send a message of a split message
  @param sess Parsing message session
  @return do keep or not to keep parsing
  */
static int zz_parse_end_json_map_split(struct zz_session *sess) {
	const int append_rc = string_append(
			&sess->json_session.http_prev_chunk.last_object,
			sess->json_session.http_chunk.in_buffer,
			yajl_get_bytes_consumed(
					sess->json_session.yajl_handler));
	if (unlikely(append_rc != 0)) {
		rdlog(LOG_ERR, "Couldn't append message (OOM?)");
		goto err;
	}

	rd_kafka_topic_t *rkt =
			topics_db_get_rdkafka_topic(sess->topic_handler);
	const int produce_rc = rd_kafka_produce_batch(
			// clang-format off
		rkt,
		RD_KAFKA_PARTITION_UA,
		RD_KAFKA_MSG_F_FREE,
		&(rd_kafka_message_t){
			.payload = sess->json_session.http_prev_chunk.last_object.buf,
			.len = string_size(&sess->json_session.http_prev_chunk.last_object),
		},
		1);
	// clang-format on

	if (likely(1 == produce_rc)) {
		// librdkafka will free it
		sess->json_session.http_prev_chunk.last_object.buf = NULL;
	}

err:
	string_done(&sess->json_session.http_prev_chunk.last_object);

	return YAJL_PARSER_OK;
}

static int zz_parse_end_json_map(void *ctx) {
	struct zz_session *sess = ctx;

	if (0 != --sess->json_session.stack_pos) {
		return YAJL_PARSER_OK;
	}

	return (string_size(&sess->json_session.http_prev_chunk.last_object) >
		0)
			       ?
			       // The message is divided between two chunks
			       zz_parse_end_json_map_split(sess)
			       :

			       zz_parse_end_json_map0(sess);
}

/** Decode a JSON chunk
    @param buffer JSONs buffer
    @param bsize buffer size
    @param session ZZ messages session
    @return DECODER_CALLBACK_OK if all went OK, DECODER_CALLBACK_INVALID_REQUEST
    if request was invalid. In latter case, session->http_response will be
    filled with JSON error
    */
static enum decoder_callback_err
process_json_buffer(const char *const_buffer,
		    size_t bsize,
		    struct zz_session *session) {
	enum decoder_callback_err rc = DECODER_CALLBACK_OK;
	if (bsize == 0) {
		return DECODER_CALLBACK_OK;
	}

	char *buffer = NULL;
	if (likely(bsize)) {
		buffer = malloc(bsize);

		if (unlikely(NULL == buffer)) {
			rdlog(LOG_ERR, "Couldn't copy buffer (OOM?)");
			rc = DECODER_CALLBACK_MEMORY_ERROR;
			goto err;
		}

		memcpy(buffer, const_buffer, bsize);
		session->json_session.http_chunk.in_buffer = buffer;
	}

	assert(session);
	yajl_status stat = yajl_parse(session->json_session.yajl_handler,
				      (const unsigned char *)buffer,
				      bsize);

	if (unlikely(stat != yajl_status_ok)) {
		static const int yajl_verbose = 1;
		char *str = (char *)yajl_get_error(
				session->json_session.yajl_handler,
				yajl_verbose,
				(const unsigned char *)const_buffer,
				bsize);
		rdlog(LOG_ERR, "Invalid entry JSON:\n%s", (const char *)str);
		string_append(&session->http_response, str, strlen(str));
		yajl_free_error(session->json_session.yajl_handler,
				(unsigned char *)str);
		rc = DECODER_CALLBACK_INVALID_REQUEST;
		goto err;
	}

	//
	// Check if we are in the middle of JSON object processing
	//

	// clang-format off
	const int append_rc =
		(session->json_session.http_chunk.last_open_map) ?
			// JSON object is not closed, save it for the next
			// decode call
			string_append(
			     &session->json_session.http_prev_chunk.last_object,
			     session->json_session.http_chunk.last_open_map,
			     bsize - (size_t)(
			       session->json_session.http_chunk.last_open_map
			       - buffer))
		: (string_size(
			&session->json_session.http_prev_chunk.last_object
			) != 0) ?
			// process_zz_buffer did not consume last object buffer,
			// so JSON object is divided in >2 chunks. Act like all
			// JSON object was in the last message!
			string_append(
			     &session->json_session.http_prev_chunk.last_object,
			     buffer,
			     bsize)
		: 0;
	// clang-format on

	if (unlikely(append_rc != 0)) {
		rdlog(LOG_ERR,
		      "Couldn't append chunked JSON object to temp buffer "
		      "(OOM?)");
		// @TODO signal error, buffer is not valid anymore!
	}

err:
	if (kafka_message_array_size(&session->kafka_msgs)) {
		kafka_message_array_set_payload_buffer(&session->kafka_msgs,
						       buffer);
	} else {
		free(buffer);
	}

	// No need for this anymore
	memset(&session->json_session.http_chunk,
	       0,
	       sizeof(session->json_session.http_chunk));

	return rc;
}

static void free_zz_session_json(struct zz_session *sess) {
	string_done(&sess->json_session.http_prev_chunk.last_object);
	yajl_free(sess->json_session.yajl_handler);
}

static string error_message_json(string error_str,
				 enum decoder_callback_err decoder_rc,
				 size_t messages_queued) {
	string ret = N2K_STRING_INITIALIZER;

	do {
		const int printf_rc = string_printf(&ret,
						    "{\"messages_queued\":%zu",
						    messages_queued);
		if (printf_rc > 0 && decoder_rc != DECODER_CALLBACK_OK) {
			string_append_string(&ret,
					     ",\"json_decoder_error\":\"");
			string_append_json_string(&ret,
						  error_str.buf,
						  string_size(&error_str));
			string_append_string(&ret, "\"");
		}

		if (printf_rc > 0) {
			string_append_string(&ret, "}");
		}
	} while (0);

	return ret;
}

/**
 * @brief      Allocates yajl JSON handler in zz session
 *
 * @param      sess  The session
 *
 * @return     0 in case of right allocation, 1 in other case.
 */
int new_zz_session_json(struct zz_session *sess) {
	static const yajl_callbacks yajl_callbacks = {
			.yajl_start_map = zz_parse_start_json_map,
			.yajl_end_map = zz_parse_end_json_map,
	};

	assert(sess);
	sess->json_session.yajl_handler =
			yajl_alloc(&yajl_callbacks, NULL, sess);
	if (NULL == sess->json_session.yajl_handler) {
		rdlog(LOG_CRIT, "Couldn't allocate yajl_handler");
		return -1;
	}

	yajl_config(sess->json_session.yajl_handler,
		    yajl_allow_multiple_values,
		    1);
	yajl_config(sess->json_session.yajl_handler,
		    yajl_allow_trailing_garbage,
		    1);

	sess->process_buffer = process_json_buffer;
	sess->error_message = error_message_json;
	sess->free_session = free_zz_session_json;

	return 0;
}
