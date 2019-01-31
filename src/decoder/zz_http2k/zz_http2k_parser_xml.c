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
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <string.h>
#include <syslog.h>
#include <threads.h>

#define NO_JSON_LAST_OPEN_MAP ((ssize_t)-1)

/// Generate a key in JSON output
#define yajl_gen_string_strlen(h, k)                                           \
	yajl_gen_string(h, (const unsigned char *)k, strlen((const char *)k))
#define yajl_gen_key_strlen yajl_gen_string_strlen

/**
 @brief      Convenience function to generate a key and value, taking
	     strlen(key) as key length

 @param[in]  gen        The YAJL generator
 @param[in]  key        The 0-terminated key
 @param[in]  value      The value
 @param[in]  value_len  The value length

 @return     yajl_gen_ok if all OK, first error in generating key/value if not
*/
static yajl_gen_status yajl_gen_key_strlen_value(yajl_gen gen,
						 const char *key,
						 const char *value,
						 size_t value_len) {
	yajl_gen_status rc =
			yajl_gen_key_strlen(gen, (const unsigned char *)key);
	if (unlikely(rc != yajl_gen_status_ok)) {
		return rc;
	}

	return yajl_gen_string(gen, (const unsigned char *)value, value_len);
}

/**
 * @brief      Generate a JSON key-value
 *
 * @param[in]  gen    The YAJL generator
 * @param[in]  key    The pair's 0-terminated key
 * @param[in]  value  The pair's 0-terminated value
 *
 * @return     yajl_gen_status_ok if all OK, proper error in other case
 */
static yajl_gen_status
yajl_gen_key_value_strlen(yajl_gen gen, const char *key, const char *value) {
	return yajl_gen_key_strlen_value(gen, key, value, strlen(value));
}

/**
 @brief      Test bit position in bool array

 @param[in]  bit_vector    The bit vector
 @param[in]  bit_position  The bit position

 @return     True if bit is set, false otherwise
*/
static bool bit_test(const char *bit_vector, size_t bit_position) {
	return ((bit_vector[bit_position / CHAR_BIT] &
		 (1 << bit_position % CHAR_BIT)) != 0);
}

/**
 @brief      Set the bit position

 @param[in]  bit_vector    The bit vector
 @param[in]  bit_position  The bit position
*/
static void bit_set(char *bit_vector, size_t bit_position) {
	bit_vector[bit_position / CHAR_BIT] |= 1 << (bit_position % CHAR_BIT);
}

/**
 @brief      Clear the bit position

 @param[in]  bit_vector    The bit vector
 @param[in]  bit_position  The bit position
*/
static void bit_clear(char *bit_vector, size_t bit_position) {
	bit_vector[bit_position / CHAR_BIT] &=
			~(1 << (bit_position % CHAR_BIT));
}

/**
 * @brief      Decoder code + error string
 */
struct code_str {
	/// Error string
	const char *err;

	/// Decoder error to return
	enum decoder_callback_err err_code;
};

static struct code_str expat_err2codestr(enum XML_Error xml_err) {
	struct code_str ret = {
			.err = XML_ErrorString(xml_err),
			.err_code = DECODER_CALLBACK_OK,
	};

	switch (xml_err) {
	case XML_ERROR_NONE:
		ret.err_code = DECODER_CALLBACK_OK;
		break;

	case XML_ERROR_NO_MEMORY:
		ret.err_code = DECODER_CALLBACK_MEMORY_ERROR;
		break;

	case XML_ERROR_SUSPENDED:
	case XML_ERROR_ABORTED:
	case XML_ERROR_FINISHED:
	case XML_ERROR_SUSPEND_PE:
	case XML_ERROR_INVALID_ARGUMENT:
	default:
		ret.err_code = DECODER_CALLBACK_GENERIC_ERROR;
		break;

	case XML_ERROR_SYNTAX:
	case XML_ERROR_NO_ELEMENTS:
	case XML_ERROR_INVALID_TOKEN:
	case XML_ERROR_UNCLOSED_TOKEN:
	case XML_ERROR_PARTIAL_CHAR:
	case XML_ERROR_TAG_MISMATCH:
	case XML_ERROR_DUPLICATE_ATTRIBUTE:
	case XML_ERROR_JUNK_AFTER_DOC_ELEMENT:
	case XML_ERROR_PARAM_ENTITY_REF:
	case XML_ERROR_UNDEFINED_ENTITY:
	case XML_ERROR_RECURSIVE_ENTITY_REF:
	case XML_ERROR_ASYNC_ENTITY:
	case XML_ERROR_BAD_CHAR_REF:
	case XML_ERROR_BINARY_ENTITY_REF:
	case XML_ERROR_ATTRIBUTE_EXTERNAL_ENTITY_REF:
	case XML_ERROR_MISPLACED_XML_PI:
	case XML_ERROR_UNKNOWN_ENCODING:
	case XML_ERROR_INCORRECT_ENCODING:
	case XML_ERROR_UNCLOSED_CDATA_SECTION:
	case XML_ERROR_EXTERNAL_ENTITY_HANDLING:
	case XML_ERROR_NOT_STANDALONE:
	case XML_ERROR_UNEXPECTED_STATE:
	case XML_ERROR_ENTITY_DECLARED_IN_PE:
	case XML_ERROR_FEATURE_REQUIRES_XML_DTD:
	case XML_ERROR_CANT_CHANGE_FEATURE_ONCE_PARSING:
	case XML_ERROR_UNBOUND_PREFIX:
	case XML_ERROR_UNDECLARING_PREFIX:
	case XML_ERROR_INCOMPLETE_PE:
	case XML_ERROR_XML_DECL:
	case XML_ERROR_TEXT_DECL:
	case XML_ERROR_PUBLICID:
	case XML_ERROR_NOT_SUSPENDED:
	case XML_ERROR_RESERVED_PREFIX_XML:
	case XML_ERROR_RESERVED_PREFIX_XMLNS:
	case XML_ERROR_RESERVED_NAMESPACE_URI:
		ret.err_code = DECODER_CALLBACK_INVALID_REQUEST;
		break;
	};

	return ret;
}

/*
 * @brief      Return the bit position on the stack that marks the stack object
 * as "text printed"
 *
 * @param[in]  stack_position  The stack position
 *
 * @return     The bit in the array that indicates if the current object has
 *             printed the object text
 */
static size_t text_printed_bit_from_frame(size_t stack_position) {
	return stack_position * 2;
}

/**
 * @brief      Mark the current stack object as "text" printed.
 *
 * @param[in]  bit_vector      The bit vector
 * @param[in]  stack_position  The JSON/XML stack position
 */
static void
set_current_stack_text_printed(char *bit_vector, size_t stack_position) {
	bit_set(bit_vector, text_printed_bit_from_frame(stack_position));
}

/**
 * @brief      Clear the current stack object has "text" key printed.
 *
 * @param[in]  bit_vector      The bit vector
 * @param[in]  stack_position  The JSON/XML stack position
 */
static void
clear_current_stack_text_printed(char *bit_vector, size_t stack_position) {
	bit_clear(bit_vector, text_printed_bit_from_frame(stack_position));
}

/**
 * @brief      Check if the current stack object has printed the "child" array.
 *
 * @param[in]  bit_vector      The bit vector
 * @param[in]  stack_position  The stack position
 *
 * @return     { description_of_the_return_value }
 */
static bool
test_current_stack_text_printed(const char *bit_vector, size_t stack_position) {
	return bit_test(bit_vector,
			text_printed_bit_from_frame(stack_position));
}

/**
 * @brief      Returns the bit position that indicates that the children JSON
 *             key has been printed
 *
 * @param[in]  stack_position  The stack position
 *
 * @return     Bit position
 */
static size_t children_printed_bit_from_frame(size_t stack_position) {
	return stack_position * 2 + 1;
}

/**
 * @brief      Mark the current stack object as "text" printed.
 *
 * @param[in]  bit_vector      The bit vector
 * @param[in]  stack_position  The JSON/XML stack position
 */
static void
set_current_stack_children_printed(char *bit_vector, size_t stack_position) {
	bit_set(bit_vector, children_printed_bit_from_frame(stack_position));
}

/**
 * @brief      Clear the current stack object has "text" key printed.
 *
 * @param[in]  bit_vector      The bit vector
 * @param[in]  stack_position  The JSON/XML stack position
 */
static void
clear_current_stack_children_printed(char *bit_vector, size_t stack_position) {
	bit_clear(bit_vector, children_printed_bit_from_frame(stack_position));
}

/**
 * @brief      Check if the current stack object has printed the "child" array.
 *
 * @param[in]  bit_vector      The bit vector
 * @param[in]  stack_position  The stack position
 *
 * @return     { description_of_the_return_value }
 */
static bool
test_current_stack_children_printed(char *bit_vector, size_t stack_position) {
	return bit_test(bit_vector,
			children_printed_bit_from_frame(stack_position));
}

/**
 @brief      Yajl gen errors strings copied from commit
	     12ee82ae5138ac86252c41f3ae8f9fd9880e4284 file yajl_gen.h

 @param[in]  s     Yajl error enumration

 @return     String describing error
*/
static struct code_str yajl_gen_err2codestr(const yajl_gen_status s) {
	switch (s) {
	case yajl_gen_status_ok:
		return (struct code_str){"No error", DECODER_CALLBACK_OK};

	case yajl_gen_keys_must_be_strings:
		// at a point where a map key is generated, a function
		// other than yajl_gen_string was called
		return (struct code_str){
				"Must generate a string here",
				DECODER_CALLBACK_GENERIC_ERROR,
		};

	case yajl_max_depth_exceeded:
		// YAJL's maximum generation depth was exceeded. See
		// YAJL_MAX_DEPTH
		return (struct code_str){
				"JSON max depth (%d) exceeded",
				DECODER_CALLBACK_INVALID_REQUEST,
		};

	case yajl_gen_in_error_state:
		// A generator function (yajl_gen_XXX) was called in an
		// error state
		return (struct code_str){
				"Trying to generate in an error state",
				DECODER_CALLBACK_GENERIC_ERROR,
		};

	case yajl_gen_generation_complete:
		// A complete JSON document has been generated
		// clang-format off
		return (struct code_str){
			"Trying to generate over a complete JSON document",
			DECODER_CALLBACK_GENERIC_ERROR,
		};
		// clang-format on

	case yajl_gen_invalid_number:
		// yajl_gen_double was passed an invalid floating point
		// value (infinity or NaN).
		return (struct code_str){
				"Invalid double on yajl_gen",
				DECODER_CALLBACK_GENERIC_ERROR,
		};

	case yajl_gen_no_buf:
		// A print callback was passed in, so there is no
		// internal buffer to get from
		return (struct code_str){
				"Trying to generate with no buffer",
				DECODER_CALLBACK_GENERIC_ERROR,
		};

	case yajl_gen_invalid_string:
		// returned from yajl_gen_string() when the
		// yajl_gen_validate_utf8 option is enabled and an
		// invalid was passed by client code.
		return (struct code_str){
				"Trying to generate an invalid utf-8 "
				"string",
				DECODER_CALLBACK_INVALID_REQUEST,
		};

	default:
		return (struct code_str){
				"Unknown error",
				DECODER_CALLBACK_GENERIC_ERROR,
		};
	};
}

/// Different decoder errors
struct xml_parser_err {
	// Error as read/interpreted by this module
	const char *error_text;

	// Error as read/interpreted by XML parser (if any)
	const char *xml_error_text;

	// Error as read/interpreted by JSON generator
	const char *json_text;

	// Return code
	enum decoder_callback_err return_code;
};

/**
 @brief      Log and return XML error

 @param      session  The session
 @param[in]  errors   The errors
*/
static void
queue_xml_error0(struct zz_session *session, struct xml_parser_err errors) {
	XML_Parser xml_parser = session->xml_session.expat_handler;

	session->xml_session.rc = errors.return_code;

	int string_printf_rc =
			string_printf(&session->http_response,
				      "<errors><error>"
				      "<what>%s</what>"
				      "<json><detail>%s</<detail></json>"
				      "<input_parser>"
				      "<detail>%s</detail>"
				      "<line>%zu</line>"
				      "<column>%zu</column>"
				      "<byte>%ld</byte></input_parser>"
				      "</error></errors>",
				      errors.error_text,
				      errors.json_text,
				      errors.xml_error_text,
				      XML_GetCurrentLineNumber(xml_parser),
				      XML_GetCurrentColumnNumber(xml_parser),
				      XML_GetCurrentByteIndex(xml_parser));

	if (unlikely(string_printf_rc < 0)) {
		rdlog(LOG_ERR, "Error printing XML error");
		return;
	}

	rdlog(LOG_ERR,
	      "%.*s",
	      (int)string_size(&session->http_response),
	      session->http_response.buf);

	XML_StopParser(xml_parser, false /* resumable */);
}

/**
 @brief      Helper macro to generate JSON elements and go to an error label in
	     error case

 @param      gen_status_var  The YAJL generator call return code
 @param      err_label       The error label to go in case of error
 @param      err_str_var     The error string variable will be set to err_str in
			     case of error
 @param      err_str         The error string in case of error
 @param      ...             YAJL generator call
*/
#define GEN_OR_GOTO_ERR(gen_status_var, err_label, err_str_var, err_str, ...)  \
	do {                                                                   \
		gen_status_var = __VA_ARGS__;                                  \
		if (unlikely(yajl_gen_status_ok != gen_status_var)) {          \
			err_str_var = err_str;                                 \
			goto err_label;                                        \
		}                                                              \
	} while (0)

/**
 @brief      Queue and log an XML error.

 @param      session          The session
 @param[in]  text             The error text
 @param[in]  yajl_gen_status  The yajl generator status
*/
static void queue_xml_error(struct zz_session *session,
			    const char *text,
			    yajl_gen_status yajl_gen_status) {
	XML_Parser xml_parser = session->xml_session.expat_handler;

	const struct code_str yajl_rc = yajl_gen_err2codestr(yajl_gen_status);
	const struct code_str xml_rc =
			expat_err2codestr(XML_GetErrorCode(xml_parser));

	const struct xml_parser_err err_s = {
			.error_text = text,
			.xml_error_text = xml_rc.err,
			.json_text = yajl_rc.err,
			.return_code = yajl_rc.err_code != DECODER_CALLBACK_OK
						       ? yajl_rc.err_code
						       : xml_rc.err_code};

	queue_xml_error0(session, err_s);
}

/**
 @brief      Starts a new XML element

 @param[in]  data  The data
 @param[in]  el    Element
 @param[in]  attr  The attribute list
*/
static void zz_parse_start_xml_element(void *data,
				       const XML_Char *el,
				       const XML_Char **attr) {
	const char *error_what = NULL;
	struct zz_session *sess = zz_session_cast(data);
	yajl_gen_status gen_status = yajl_gen_status_ok;

	if (unlikely(string_size(&sess->http_response) > 0)) {
		// An error has already been queued
		return;
	}

	yajl_gen yajl_gen = sess->xml_session.yajl_gen;

	if (sess->xml_session.json_buf.stack_pos == 0) {
		sess->xml_session.json_buf.last_open_map = (ssize_t)string_size(
				&sess->xml_session.json_buf.yajl_gen_buf);
	} else {
		if (!test_current_stack_children_printed(
				    sess->xml_session.json_buf
						    .stack_boolean_info,
				    sess->xml_session.json_buf.stack_pos)) {
			GEN_OR_GOTO_ERR(gen_status,
					err,
					error_what,
					"Can't gen JSON children key",
					yajl_gen_key_strlen(yajl_gen,
							    "children"));
			GEN_OR_GOTO_ERR(gen_status,
					err,
					error_what,
					"Can't gen JSON child array open "
					"bracket",
					yajl_gen_array_open(yajl_gen));

			set_current_stack_children_printed(
					sess->xml_session.json_buf
							.stack_boolean_info,
					sess->xml_session.json_buf.stack_pos);
		} else {
			// Need to close previous child, we know for sure that
			// there is no trailing text
			GEN_OR_GOTO_ERR(gen_status,
					err,
					error_what,
					"Can't gen JSON child array close "
					"brace",
					yajl_gen_map_close(yajl_gen));
		}
	}

	sess->xml_session.json_buf.stack_pos++;

	clear_current_stack_text_printed(
			sess->xml_session.json_buf.stack_boolean_info,
			sess->xml_session.json_buf.stack_pos);
	clear_current_stack_children_printed(
			sess->xml_session.json_buf.stack_boolean_info,
			sess->xml_session.json_buf.stack_pos);

	GEN_OR_GOTO_ERR(gen_status,
			err,
			error_what,
			"Can't gen JSON object open brace",
			yajl_gen_map_open(yajl_gen));

	GEN_OR_GOTO_ERR(gen_status,
			err,
			error_what,
			"Can't gen JSON text",
			yajl_gen_key_value_strlen(yajl_gen, "tag", el));

	size_t i;
	for (i = 0; attr[i]; i += 2) {
		assert(attr[i]);
		assert(attr[i + 1]);

		if (i == 0) {
			GEN_OR_GOTO_ERR(gen_status,
					err,
					error_what,
					"Can't gen JSON attributes key",
					yajl_gen_key_strlen(yajl_gen,
							    "attributes"));
			GEN_OR_GOTO_ERR(gen_status,
					err,
					error_what,
					"Can't gen JSON attributes open brace",
					yajl_gen_map_open(yajl_gen));
		}

		GEN_OR_GOTO_ERR(gen_status,
				err,
				error_what,
				"Can't gen JSON attribute key + val",
				yajl_gen_key_value_strlen(yajl_gen,
							  attr[i],
							  attr[i + 1]));

		if (attr[i + 2] == NULL) {
			GEN_OR_GOTO_ERR(gen_status,
					err,
					error_what,
					"Can't gen JSON close brace",
					yajl_gen_map_close(yajl_gen));
		}
	}

	return;

err:
	queue_xml_error(sess, error_what, gen_status);
	sess->xml_session.json_buf.last_open_map = NO_JSON_LAST_OPEN_MAP;
	return;
}

static void zz_parse_end_xml_element(void *data, const XML_Char *element_name) {
	(void)element_name;
	struct zz_session *sess = zz_session_cast(data);
	const char *error_what = NULL;
	yajl_gen_status gen_status = yajl_gen_status_ok;
	yajl_gen yajl_gen = sess->xml_session.yajl_gen;

	if (unlikely(string_size(&sess->http_response) > 0)) {
		// An error has already been queued
		return;
	}

	if (test_current_stack_children_printed(
			    sess->xml_session.json_buf.stack_boolean_info,
			    sess->xml_session.json_buf.stack_pos)) {
		// Close last child
		GEN_OR_GOTO_ERR(gen_status,
				err,
				error_what,
				"Can't gen child JSON close key",
				yajl_gen_map_close(yajl_gen));

		GEN_OR_GOTO_ERR(gen_status,
				err,
				error_what,
				"Can't gen JSON child array close bracket",
				yajl_gen_array_close(yajl_gen));
	}

	// We should close the JSON object here, but text can come after this
	// close tag. Instead, we will close the (child) JSON object when:
	// - New object (child) is appended
	// - Close the root object (if we are it)

	if (--sess->xml_session.json_buf.stack_pos > 0) {
		// Not last element
		return;
	}

	// Need to add last close
	GEN_OR_GOTO_ERR(gen_status,
			err,
			error_what,
			"Can't gen child JSON close key",
			yajl_gen_map_close(yajl_gen));

	const size_t buf_siz =
			string_size(&sess->xml_session.json_buf.yajl_gen_buf);

	assert(sess->xml_session.json_buf.last_open_map >= 0);
	assert(buf_siz > (size_t)sess->xml_session.json_buf.last_open_map);

	rd_kafka_message_t msg = {
			// Only offset, buffer can change in reallocations
			.payload = (char *)sess->xml_session.json_buf
						   .last_open_map,
			.len = buf_siz -
			       (size_t)sess->xml_session.json_buf.last_open_map,
	};

	const int add_rc = kafka_msg_array_add(&sess->kafka_msgs, &msg);
	sess->xml_session.json_buf.last_open_map = NO_JSON_LAST_OPEN_MAP;

	yajl_gen_reset(yajl_gen, NULL);

	if (unlikely(add_rc != 0)) {
		queue_xml_error(sess,
				"Couldn't add kafka message (OOM?)",
				yajl_gen_status_ok);
	}

	XML_StopParser(sess->xml_session.expat_handler, false /* resumable */);
	return;

err:
	queue_xml_error(sess, error_what, gen_status);
	sess->xml_session.json_buf.last_open_map = NO_JSON_LAST_OPEN_MAP;
}

/// Pthread key, only used for the destructor property
static pthread_key_t thread_local_gen_key;

/**
 @brief      Free the yajl_gen. Only used for "casting" the pthread_key
	     destructor callback

 @param      yajl_gen  The yajl generator
*/
static void void_yajl_gen_free(void *yajl_gen) {
	yajl_gen_free(yajl_gen);
}

/**
 * @brief      Initializes thread_local_gen_key.
 */
static void thread_local_gen_key_init(void) {
	pthread_key_create(&thread_local_gen_key, void_yajl_gen_free);
}

/**
 @brief      Parse text character data

 @param[in]  user_data  The handler
 @param[in]  text       The character buffer
 @param[in]  text_len   The text length
*/
static void zz_parse_xml_character_data(void *user_data,
					const XML_Char *text,
					int text_len) {

	static const yajl_alloc_funcs *allocFuncs = NULL;

	struct zz_session *sess = zz_session_cast(user_data);

	assert(sess);
	assert(text);
	assert(text_len >= 0);
	const char *error_what = NULL;
	yajl_gen_status gen_status = yajl_gen_status_ok;

	const size_t printing_text_bit_pos =
			sess->xml_session.json_buf.stack_pos;

	// This callback is called multiple times, incrementally, while we are
	// parsing text. So we need to differentiate here if we are starting to
	// print text or, on the contrary, we are in the successive calls
	if (!test_current_stack_text_printed(
			    sess->xml_session.json_buf.stack_boolean_info,
			    printing_text_bit_pos)) {

		// First call

		// clang-format off
		const char *key = test_current_stack_children_printed(
				sess->xml_session.json_buf.stack_boolean_info,
				printing_text_bit_pos)
			? "tail"
			: "text";
		// clang-format off

		GEN_OR_GOTO_ERR(gen_status,
				err,
				error_what,
				"Can't gen JSON attribute key + val in XML tag "
				"text",
				yajl_gen_key_strlen_value(
						sess->xml_session.yajl_gen,
						key,
						text,
						(size_t)text_len));

		set_current_stack_text_printed(
				sess->xml_session.json_buf.stack_boolean_info,
				printing_text_bit_pos);

		return;
	}

	// Not first call
	thread_local static yajl_gen temp_yajl = NULL;
	if (unlikely(NULL == temp_yajl)) {
		static pthread_once_t key_once = PTHREAD_ONCE_INIT;
		pthread_once(&key_once, thread_local_gen_key_init);

		temp_yajl = yajl_gen_alloc(allocFuncs);
		pthread_setspecific(thread_local_gen_key, temp_yajl);

		if (unlikely(NULL == temp_yajl)) {
			error_what = "Can't allocate temporary JSON "
				     "generator (OOM?)";
			goto err;
		}
	}

	yajl_gen_reset(temp_yajl, "");
	yajl_gen_clear(temp_yajl);

	GEN_OR_GOTO_ERR(gen_status,
			err,
			error_what,
			"Can't generate XML text in JSON string",
			yajl_gen_string(temp_yajl,
					(const unsigned char *)text,
					(size_t)text_len));

	const char *yajl_buf;
	size_t yajl_buf_size;
	yajl_gen_get_buf(temp_yajl,
			 (const unsigned char **)&yajl_buf,
			 &yajl_buf_size);

	assert(yajl_buf);

	string_pop_back(&sess->xml_session.json_buf.yajl_gen_buf);
	string_append(&sess->xml_session.json_buf.yajl_gen_buf,
		      yajl_buf + 1 /* skip first quote */,
		      yajl_buf_size - 1);

err:
	if (unlikely(error_what)) {
		queue_xml_error(sess, error_what, yajl_gen_status_ok);
	}
}

/**
 @brief      Reset the parser and set the user data and handlers
	     (XML_ParserReset does delete them). If the parser is just
	     allocated, there is no need of do a full reset, so set the
	     do_parser_internal_reset to false

 @param[in]  xml_parser              The xml parser
 @param      user_data               The user data to set to xml parser
				     callbacks
 @param[in]  do_parser_status_reset  Make an internal fields reset. No need if
				     the parser has been just allocated.
*/
static void zz_reset_xml_handler(XML_Parser xml_parser,
				 void *user_data,
				 bool do_parser_status_reset) {
	// No way this can return false in libexpat-2.2.6 with a valid
	// xml_parser and no parent parser
	if (do_parser_status_reset) {
		XML_ParserReset(xml_parser, NULL /* encoding */);
	}

	XML_SetUserData(xml_parser, user_data);
	XML_SetElementHandler(xml_parser,
			      zz_parse_start_xml_element,
			      zz_parse_end_xml_element);
	XML_SetCharacterDataHandler(xml_parser, zz_parse_xml_character_data);
}

/** Decode an XML chunk

 @param      buffer   JSONs buffer
 @param      bsize    buffer size
 @param      session  ZZ messages session

 @return     DECODER_CALLBACK_OK if all went OK,
	     DECODER_CALLBACK_INVALID_REQUEST if request was invalid. In latter
	     case, session->http_response will be filled with JSON error
*/
static enum decoder_callback_err
process_xml_buffer(const char *buffer,
		   size_t bsize,
		   struct zz_session *session) {

	assert(session);

	XML_Parser xml_parser = session->xml_session.expat_handler;

	while (bsize > 0 && 0 == string_size(&session->http_response)) {
		// XML_Parse callbacks will suspend the handler every time a
		// complete object is processed, so we can restart the handler
		// here
		const enum XML_Status stat = XML_Parse(
				xml_parser, buffer, bsize, false /* done */);

		// clang-format off
		const bool true_error =
			(stat == XML_STATUS_SUSPENDED) ?
				(NULL != session->http_response.buf)
			: (stat != XML_STATUS_ERROR) ?
				false
			: XML_GetErrorCode(xml_parser) !=
				XML_ERROR_JUNK_AFTER_DOC_ELEMENT &&
			  XML_GetErrorCode(xml_parser) !=
			  	XML_ERROR_ABORTED;
		// clang-format on

		if (stat == XML_STATUS_OK) {
			bsize = 0;
		} else if (unlikely(true_error)) {
			queue_xml_error(session,
					"Error parsing XML",
					yajl_gen_status_ok);
			break;
		} else {
			// Junk should mean that we have more XML trees to parse
			const XML_Index offset =
					XML_GetCurrentByteIndex(xml_parser);
			if (unlikely(offset <= 0 &&
				     0 == string_size(&session->http_response))) {
				queue_xml_error(session,
						"Can't get parse offset",
						yajl_gen_status_ok);
				break;
			}

			assert((size_t)offset <= bsize);
			buffer += offset;
			bsize -= (size_t)offset;

			zz_reset_xml_handler(xml_parser, session, true);
		}
	}

	if (kafka_message_array_size(&session->kafka_msgs)) {
		kafka_message_array_set_payload_buffer0(
				&session->kafka_msgs,
				session->xml_session.json_buf.yajl_gen_buf.buf,
				true /* sum_offset */);
	} else {
		string_done(&session->xml_session.json_buf.yajl_gen_buf);
		memset(&session->xml_session.json_buf.yajl_gen_buf,
		       0,
		       sizeof(session->xml_session.json_buf.yajl_gen_buf));
	}

	session->xml_session.json_buf.yajl_gen_buf = N2K_STRING_INITIALIZER;
	return session->xml_session.rc;
}

static void free_zz_session_xml(struct zz_session *sess) {
	yajl_gen_free(sess->xml_session.yajl_gen);
	XML_ParserFree(sess->xml_session.expat_handler);
}

static string xml_error_message(string error_str,
				enum decoder_callback_err decoder_rc,
				size_t messages_queued) {

	string ret = N2K_STRING_INITIALIZER;

	string_printf(&ret,
		      "<result><messages_queued>%zu"
		      "</messages_queued>"
		      "%.*s"
		      "</result>",
		      messages_queued,

		      decoder_rc == DECODER_CALLBACK_OK
				      ? 0
				      : (int)string_size(&error_str),
		      decoder_rc == DECODER_CALLBACK_OK ? NULL : error_str.buf);

	return ret;
}

static void yajl_print_callback(void *ctx, const char *str, size_t len) {
	struct zz_session *sess = zz_session_cast(ctx);

	const int append_rc = string_append(
			&sess->xml_session.json_buf.yajl_gen_buf, str, len);

	if (unlikely(append_rc != 0)) {
		queue_xml_error(sess,
				"Unable to append JSON text (OOM?)",
				yajl_gen_status_ok);
	}
}

int new_zz_session_xml(struct zz_session *sess) {
	static const XML_Char *encoding_auto = NULL;
	static const yajl_alloc_funcs *allocFuncs = NULL;

	assert(sess);
	sess->xml_session.expat_handler = XML_ParserCreate(encoding_auto);

	if (unlikely(NULL == sess->xml_session.expat_handler)) {
		rdlog(LOG_ERR, "Can't allocate XML handler (OOM?)");
		return -1;
	}

	zz_reset_xml_handler(sess->xml_session.expat_handler, sess, false);

	sess->xml_session.yajl_gen = yajl_gen_alloc(allocFuncs);
	if (unlikely(NULL == sess->xml_session.yajl_gen)) {
		rdlog(LOG_ERR, "Can't allocate JSON generator handler (OOM?)");
		XML_ParserFree(sess->xml_session.expat_handler);
		goto yajl_gen_err;
	}

	yajl_gen_config(sess->xml_session.yajl_gen,
			yajl_gen_print_callback,
			yajl_print_callback,
			sess);
	sess->xml_session.json_buf.last_open_map = NO_JSON_LAST_OPEN_MAP;
	sess->process_buffer = process_xml_buffer;
	sess->free_session = free_zz_session_xml;
	sess->error_message = xml_error_message;

	return 0;

yajl_gen_err:
	XML_ParserFree(sess->xml_session.expat_handler);

	return -1;
}
