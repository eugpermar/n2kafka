/*
**
** Copyright (C) 2014-2016, Eneo Tecnologia S.L.
** Copyright (C) 2017, Eugenio Perez <eupm90@gmail.com>
** Copyright (C) 2018, Wizzie S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
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

#pragma once

#include "config.h"

#include "util/string.h"
#include "util/util.h"

#include <librdkafka/rdkafka.h>
#include <time.h>

#include <assert.h>
#include <stdlib.h>

/// Internal kafka message array definition
struct kafka_message_array_internal {
#ifndef NDEBUG
#define KAFKA_MESSAGE_ARRAY_INTERNAL_MAGIC 0xaa343aa13aaa343aL
	uint64_t magic;
#endif
	void *payload_buffer;      ///< Messages payload buffer
	size_t count;		   ///< Producer side: Messages count
	rd_kafka_message_t msgs[]; /// Actual kafka messages
};

static void assert_kafka_message_array_internal(
		const struct kafka_message_array_internal *karray) {
#ifdef KAFKA_MESSAGE_ARRAY_INTERNAL_MAGIC
	assert(KAFKA_MESSAGE_ARRAY_INTERNAL_MAGIC == karray->magic);
#else
	(void)karray;
#endif
}

/// Cast void pointer to message array internal, checking consistency magic if
/// not NDEBUG defined
#define kafka_message_array_internal_cast0(t_qual, opaque)                     \
	({                                                                     \
		t_qual struct kafka_message_array_internal *ret = opaque;      \
		assert_kafka_message_array_internal(ret);                      \
		ret;                                                           \
	})

/// Cast void pointer to message array internal
static struct kafka_message_array_internal *
kafka_message_array_internal_cast(void *opaque) RD_UNUSED;
static struct kafka_message_array_internal *
kafka_message_array_internal_cast(void *opaque) {
	return kafka_message_array_internal_cast0(, opaque);
}

/// Cast void pointer to message array internal, const version
static const struct kafka_message_array_internal *
kafka_message_array_internal_cast_const(void *opaque) RD_UNUSED;
static const struct kafka_message_array_internal *
kafka_message_array_internal_cast_const(void *opaque) {
	return kafka_message_array_internal_cast0(, opaque);
}

/// Kafka message array
typedef struct kafka_message_array {
	/// Internal byte buffer
	/// @note Internal - Do not use directly
	string str;
} kafka_message_array;

#define KAFKA_MESSAGE_ARRAY_INITIALIZER                                        \
	(kafka_message_array) {                                                \
		.str = N2K_STRING_INITIALIZER,                                 \
	}

/** Return private array
  @note: Internal - Do not use outside this header
  */
static struct kafka_message_array_internal *
kafka_message_array_get_internal(kafka_message_array *array) RD_UNUSED;
static struct kafka_message_array_internal *
kafka_message_array_get_internal(kafka_message_array *array) {
	return kafka_message_array_internal_cast(array->str.buf);
}

/** Return private array
  @note: Internal - Do not use outside this header
  */
static const struct kafka_message_array_internal *
kafka_message_array_get_internal_const(
		const kafka_message_array *array) RD_UNUSED;
static const struct kafka_message_array_internal *
kafka_message_array_get_internal_const(const kafka_message_array *array) {
	return kafka_message_array_internal_cast_const(array->str.buf);
}

static size_t kafka_message_array_size(const kafka_message_array *array)
		__attribute__((unused));
static size_t kafka_message_array_size(const kafka_message_array *array) {
	assert(array);
	return string_size(&array->str)
			       ? kafka_message_array_get_internal_const(array)
						 ->count
			       : 0;
}

static void
kafka_message_array_set_payload_buffer(kafka_message_array *array,
				       char *payload_buffer) RD_UNUSED;
static void kafka_message_array_set_payload_buffer(kafka_message_array *array,
						   char *payload_buffer) {
	kafka_message_array_get_internal(array)->payload_buffer =
			payload_buffer;
}

#define X_RK_M_PRODUCE_ERR(X)                                                  \
	X(RD_KAFKA_RESP_ERR__QUEUE_FULL, LAST_WARNING_TIME__QUEUE_FULL)        \
	X(RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE,                                \
	  LAST_WARNING_TIME__MSG_SIZE_TOO_LARGE)                               \
	X(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,                                \
	  LAST_WARNING_TIME__UNKNOWN_PARTITION)                                \
	X(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC, LAST_WARNING_TIME__UNKNOWN_TOPIC)

enum warning_times_pos {
#define LAST_WARNING_POS_ENUM(RK_ERR, N2K_ERR) N2K_ERR,
	X_RK_M_PRODUCE_ERR(LAST_WARNING_POS_ENUM) LAST_WARNING_TIME__END,
};

/// Information to not to saturate log output with error messages
typedef struct kafka_message_array_last_warning_state {
	/// Array of warning last timestamp
	time_t produce_error_last_time[LAST_WARNING_TIME__END];
} kafka_message_array_produce_state;

/** Produce a message array
  @param topic Topic to produce messages
  @param array Message array
  @param common payload buffer. It will be freed when all messages has been
  delivered
  @param rdkafka_flags Flags to send to librdkafka.
  @param state
  @return Number of messages effectively queued
  @warning this function consumes all the array, queued or not.
  */
size_t kafka_message_array_produce(rd_kafka_topic_t *topic,
				   kafka_message_array *array,
				   char *payload_buffer,
				   int rdkafka_flags,
				   kafka_message_array_produce_state *state);

/** Init a message queue
	@param q Queue */
static void
kafka_msg_array_init(kafka_message_array *array) __attribute__((unused));
static void kafka_msg_array_init(kafka_message_array *array) {
	string_init(&array->str);
}

static void kafka_msg_array_done(kafka_message_array *array) RD_UNUSED;
static void kafka_msg_array_done(kafka_message_array *array) {
	string_done(&array->str);
}

/** Add a message to the message array
	@param array Queue
	@param msg Message
	@return 0 if ok, other if no memory avalable */
static int
kafka_msg_array_add(kafka_message_array *array, const rd_kafka_message_t *msg)
		__attribute__((unused));
static int
kafka_msg_array_add(kafka_message_array *array, const rd_kafka_message_t *msg) {
	if (0 == kafka_message_array_size(array)) {
		static const struct kafka_message_array_internal karray_init = {
#ifdef KAFKA_MESSAGE_ARRAY_INTERNAL_MAGIC
				.magic = KAFKA_MESSAGE_ARRAY_INTERNAL_MAGIC,
#endif
		};
		const int create_rc = string_append(&array->str,
						    (const char *)&karray_init,
						    sizeof(karray_init));
		if (unlikely(create_rc != 0)) {
			return create_rc;
		}
	}

	const int rc = string_append(
			&array->str, (const char *)msg, sizeof(*msg));
	if (likely(rc == 0)) {
		kafka_message_array_get_internal(array)->count++;
	}

	return rc;
}

/** Decrement reference counter in a thread-safe manner
  @param array Kafka messages array
  */
static void
kafka_message_array_internal_decref(struct kafka_message_array_internal *karray)
		__attribute__((unused));
static void kafka_message_array_internal_decref(
		struct kafka_message_array_internal *karray) {
	if (0 == ATOMIC_OP(sub, fetch, &karray->count, 1)) {
		free(karray->payload_buffer);
		free(karray);
	}
}
