/*
** Copyright (C) 2014-2016, Eneo Tecnologia S.L.
** Copyright (C) 2017, Eugenio Perez <eupm90@gmail.com>
** Author: Eugenio Perez <eupm90@gmail.com>
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
#include "engine/parse.h"
#include "util/pair.h"
#include <librdkafka/rdkafka.h>

#include <string.h>

/* Private data */
struct rd_kafka_message_s;

struct kafka_message_array {
	size_t count;			 /* Number of used elements in msgs */
	size_t size;			 /* Number of elements in msgs */
	struct rd_kafka_message_s *msgs; /* Real msgs */
};

void init_rdkafka();
void send_to_kafka(rd_kafka_topic_t *rkt,
		   char *buffer,
		   const size_t bufsize,
		   int flags,
		   void *opaque);

/// @TODO join with zz_http2k_decoder mac partitioner
int32_t rb_client_mac_partitioner(const rd_kafka_topic_t *_rkt,
				  const void *key,
				  size_t keylen,
				  int32_t partition_cnt,
				  void *rkt_opaque,
				  void *msg_opaque);

struct kafka_message_array *new_kafka_message_array(size_t size);
int save_kafka_msg_key_partition_in_array(struct kafka_message_array *array,
					  char *key,
					  size_t key_size,
					  char *buffer,
					  size_t buf_size,
					  int partition,
					  void *opaque);
#define save_kafka_msg_key_in_array(                                           \
		array, key, key_size, buffer, buf_size, opaque)                \
	save_kafka_msg_key_partition_in_array(array,                           \
					      key,                             \
					      key_size,                        \
					      buffer,                          \
					      buf_size,                        \
					      RD_KAFKA_PARTITION_UA,           \
					      opaque)
#define save_kafka_msg_in_array(array, buffer, buf_size, opaque)               \
	save_kafka_msg_key_in_array(array, NULL, 0, buffer, buf_size, opaque);

/** Send an array of messages to a given topic. Messages that couldn't be
  sent will be freed, and error message will be shown
  @param rkt Topic to send messages
  @param msgs Messages to be sent
  @return Number of messages sent
  */
int send_array_to_kafka(rd_kafka_topic_t *rkt,
			struct kafka_message_array *msgs);

void kafka_poll();

typedef int32_t (*rb_rd_kafka_partitioner_t)(const rd_kafka_topic_t *rkt,
					     const void *keydata,
					     size_t keylen,
					     int32_t partition_cnt,
					     void *rkt_opaque,
					     void *msg_opaque);

/** Creates a new topic handler using global configuration
    @param topic_name Topic name
    @param partitioner Partitioner function
    @return New topic handler */
rd_kafka_topic_t *new_rkt_global_config(const char *topic_name,
					rb_rd_kafka_partitioner_t partitioner);

/** Default kafka topic name (if any)
	@return Default kafka topic name (if any)
	*/
const char *default_topic_name();

void flush_kafka();
void stop_rdkafka();
