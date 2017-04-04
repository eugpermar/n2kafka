/*
**
** Copyright (C) 2014-2016, Eneo Tecnologia S.L.
** Copyright (C) 2017, Eugenio Perez <eupm90@gmail.com>
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

#include "zz_http2k_decoder.h"
#include "engine/global_config.h"
#include "util/kafka.h"
#include "util/kafka_message_list.h"
#include "util/rb_json.h"
#include "util/rb_mac.h"
#include "util/rb_time.h"
#include "util/topic_database.h"
#include "util/util.h"
#include "zz_http2k_parser.h"
#include "zz_http2k_sync_common.h"

#include <assert.h>
#include <errno.h>
#include <jansson.h>
#include <librd/rd.h>
#include <librd/rdlog.h>
#include <librd/rdmem.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>

static const char RB_HTTP2K_CONFIG_KEY[] = "zz_http2k_config";
static const char RB_SENSORS_UUID_KEY[] = "sensors_uuids";
static const char RB_ORGANIZATIONS_UUID_KEY[] = "organizations_uuids";
static const char RB_N2KAFKA_ID[] = "n2kafka_id";
static const char RB_ORGANIZATIONS_SYNC_KEY[] = "organizations_sync";
static const char RB_ORGANIZATIONS_SYNC_TOPICS_KEY[] = "topics";
static const char RB_ORGANIZATIONS_SYNC_INTERVAL_S_KEY[] = "interval_s";
static const char RB_ORGANIZATIONS_SYNC_CLEAN_KEY[] = "clean_on";
static const char RB_ORGANIZATIONS_SYNC_CLEAN_TIMESTAMP_MOD_KEY[] =
		"timestamp_s_mod";
static const char RB_ORGANIZATIONS_SYNC_CLEAN_TIMESTAMP_OFFSET_KEY[] =
		"timestamp_s_offset";
static const char RB_ORGANIZATIONS_SYNC_PUT_URL_KEY[] = "put_url";
static const char RB_TOPICS_KEY[] = "topics";
static const char RB_SENSOR_UUID_KEY[] = "uuid";

static const char RB_TOPIC_PARTITIONER_KEY[] = "partition_key";
static const char RB_TOPIC_PARTITIONER_ALGORITHM_KEY[] = "partition_algo";

typedef int32_t (*partitioner_cb)(const rd_kafka_topic_t *rkt,
				  const void *keydata,
				  size_t keylen,
				  int32_t partition_cnt,
				  void *rkt_opaque,
				  void *msg_opaque);

static int32_t(mac_partitioner)(const rd_kafka_topic_t *rkt,
				const void *keydata,
				size_t keylen,
				int32_t partition_cnt,
				void *rkt_opaque,
				void *msg_opaque);

/** Algorithm of messages partitioner */
enum partitioner_algorithm {
	/** Random partitioning */
	none,
	/** Mac partitioning */
	mac,
};

struct {
	enum partitioner_algorithm algoritm;
	const char *name;
	partitioner_cb partitioner;
} partitioner_algorithm_list[] = {
		{mac, "mac", mac_partitioner},
};

enum warning_times_pos {
	LAST_WARNING_TIME__QUEUE_FULL,
	LAST_WARNING_TIME__MSG_SIZE_TOO_LARGE,
	LAST_WARNING_TIME__UNKNOWN_PARTITION,
	LAST_WARNING_TIME__UNKNOWN_TOPIC,
	LAST_WARNING_TIME__END
};

struct zz_opaque {
#ifndef NDEBUG
#define ZZ_OPAQUE_MAGIC 0x0B0A3A1C0B0A3A1CL
	uint64_t magic;
#endif

	struct zz_config *zz_config;

	pthread_mutex_t produce_error_last_time_mutex[LAST_WARNING_TIME__END];
	time_t produce_error_last_time[LAST_WARNING_TIME__END];
};

#ifdef ZZ_OPAQUE_MAGIC
static void assert_zz_opaque(struct zz_opaque *zz_opaque) {
	assert(zz_opaque);
	assert(ZZ_OPAQUE_MAGIC == zz_opaque->magic);
}
#else
#define assert_zz_opaque(zz_opaque) (void)zz_opaque
#endif

static enum warning_times_pos
kafka_error_to_warning_time_pos(rd_kafka_resp_err_t err) {
	switch (err) {
	case RD_KAFKA_RESP_ERR__QUEUE_FULL:
		return LAST_WARNING_TIME__QUEUE_FULL;
	case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
		return LAST_WARNING_TIME__MSG_SIZE_TOO_LARGE;
	case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
		return LAST_WARNING_TIME__UNKNOWN_PARTITION;
	case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
		return LAST_WARNING_TIME__UNKNOWN_TOPIC;
	default:
		return LAST_WARNING_TIME__END;
	};
}

static int32_t mac_partitioner(const rd_kafka_topic_t *rkt,
			       const void *keydata,
			       size_t keylen,
			       int32_t partition_cnt,
			       void *rkt_opaque,
			       void *msg_opaque) {
	size_t toks = 0;
	uint64_t intmac = 0;
	char mac_key[sizeof("00:00:00:00:00:00")];

	if (keylen != strlen("00:00:00:00:00:00")) {
		if (keylen != 0) {
			/* We were expecting a MAC and we do not have it */
			rdlog(LOG_WARNING,
			      "Invalid mac %.*s len",
			      (int)keylen,
			      (const char *)keydata);
		}
		goto fallback_behavior;
	}

	mac_key[0] = '\0';
	strncat(mac_key, (const char *)keydata, sizeof(mac_key) - 1);

	for (toks = 1; toks < 6; ++toks) {
		const size_t semicolon_pos = 3 * toks - 1;
		if (':' != mac_key[semicolon_pos]) {
			rdlog(LOG_WARNING,
			      "Invalid mac %.*s (it does not have ':' in char "
			      "%zu.",
			      (int)keylen,
			      mac_key,
			      semicolon_pos);
			goto fallback_behavior;
		}
	}

	for (toks = 0; toks < 6; ++toks) {
		char *endptr = NULL;
		intmac = (intmac << 8) +
			 strtoul(&mac_key[3 * toks], &endptr, 16);
		/// The key should end with '"' in json format
		if ((toks < 5 && *endptr != ':') ||
		    (toks == 5 && *endptr != '\0')) {
			rdlog(LOG_WARNING,
			      "Invalid mac %.*s, unexpected %c end of %zu "
			      "token",
			      (int)keylen,
			      mac_key,
			      *endptr,
			      toks);
			goto fallback_behavior;
		}

		if (endptr != mac_key + 3 * (toks + 1) - 1) {
			rdlog(LOG_WARNING,
			      "Invalid mac %.*s, unexpected token length at "
			      "%zu token",
			      (int)keylen,
			      mac_key,
			      toks);
			goto fallback_behavior;
		}
	}

	return intmac % (uint32_t)partition_cnt;

fallback_behavior:
	return rd_kafka_msg_partitioner_random(rkt,
					       keydata,
					       keylen,
					       partition_cnt,
					       rkt_opaque,
					       msg_opaque);
}

/** Parsing of per uuid enrichment.
	@param config original config with RB_SENSORS_UUID_KEY to extract it.
	@return new uuid database
	*/
static sensors_db_t *
parse_per_uuid_opaque_config(json_t *config,
			     organizations_db_t *organizations_db) {
	assert(config);

	json_t *sensors_config = json_object_get(config, RB_SENSORS_UUID_KEY);

	if (NULL == sensors_config) {
		rdlog(LOG_ERR,
		      "Couldn't unpack %s key: Object not found",
		      RB_SENSORS_UUID_KEY);
		return NULL;
	}

	return sensors_db_new(sensors_config, organizations_db);
}

static partitioner_cb partitioner_of_name(const char *name) {
	size_t i = 0;

	assert(name);

	for (i = 0; i < RD_ARRAYSIZE(partitioner_algorithm_list); ++i) {
		if (0 == strcmp(partitioner_algorithm_list[i].name, name)) {
			return partitioner_algorithm_list[i].partitioner;
		}
	}

	return NULL;
}

static int
parse_topic_list_config(json_t *config, struct topics_db *new_topics_db) {
	const char *key;
	json_t *value;
	json_error_t jerr;
	json_t *topic_list = NULL;
	int pass = 0;

	assert(config);

	const int json_unpack_rc = json_unpack_ex(
			config, &jerr, 0, "{s:o}", RB_TOPICS_KEY, &topic_list);

	if (0 != json_unpack_rc) {
		rdlog(LOG_ERR, "%s", jerr.text);
		return json_unpack_rc;
	}

	if (!json_is_object(topic_list)) {
		rdlog(LOG_ERR, "%s is not an object", RB_TOPICS_KEY);
		return -1;
	}

	const size_t array_size = json_object_size(topic_list);
	if (0 == array_size) {
		rdlog(LOG_ERR, "%s has no childs", RB_TOPICS_KEY);
		return -1;
	}

	json_object_foreach(topic_list, key, value) {
		const char *partition_key = NULL, *partition_algo = NULL;
		size_t partition_key_len = 0;
		const char *topic_name = key;
		rd_kafka_topic_t *rkt = NULL;

		if (!json_is_object(value)) {
			if (pass == 0) {
				rdlog(LOG_ERR,
				      "Topic %s is not an object. Discarding.",
				      topic_name);
			}
			continue;
		}

		const int topic_unpack_rc = json_unpack_ex(
				value,
				&jerr,
				0,
				"{s?s%,s?s}",
				RB_TOPIC_PARTITIONER_KEY,
				&partition_key,
				&partition_key_len,
				RB_TOPIC_PARTITIONER_ALGORITHM_KEY,
				&partition_algo);

		if (0 != topic_unpack_rc) {
			rdlog(LOG_ERR,
			      "Can't extract information of topic %s (%s). "
			      "Discarding.",
			      topic_name,
			      jerr.text);
			continue;
		}

		rd_kafka_topic_conf_t *my_rkt_conf = rd_kafka_topic_conf_dup(
				global_config.kafka_topic_conf);
		if (NULL == my_rkt_conf) {
			rdlog(LOG_ERR,
			      "Couldn't topic_conf_dup in topic %s",
			      topic_name);
			continue;
		}

		if (NULL != partition_algo) {
			partitioner_cb partitioner =
					partitioner_of_name(partition_algo);
			if (NULL != partitioner) {
				rd_kafka_topic_conf_set_partitioner_cb(
						my_rkt_conf, partitioner);
			} else {
				rdlog(LOG_ERR,
				      "Can't found partitioner algorithm %s "
				      "for topic %s",
				      partition_algo,
				      topic_name);
			}
		}

		rkt = rd_kafka_topic_new(
				global_config.rk, topic_name, my_rkt_conf);
		if (NULL == rkt) {
			char buf[BUFSIZ];
			strerror_r(errno, buf, sizeof(buf));
			rdlog(LOG_ERR,
			      "Can't create topic %s: %s",
			      topic_name,
			      buf);
			rd_kafka_topic_conf_destroy(my_rkt_conf);
			continue;
		}

		topics_db_add(new_topics_db,
			      rkt,
			      partition_key,
			      partition_key_len);
	}

	return 0;
}

int zz_opaque_creator(json_t *config __attribute__((unused)), void **_opaque) {
	size_t i;

	assert(_opaque);

	struct zz_opaque *opaque = (*_opaque) = calloc(1, sizeof(*opaque));
	if (NULL == opaque) {
		rdlog(LOG_ERR, "Can't alloc RB_HTTP2K opaque (out of memory?)");
		return -1;
	}

#ifdef ZZ_OPAQUE_MAGIC
	opaque->magic = ZZ_OPAQUE_MAGIC;
#endif

	for (i = 0; i < RD_ARRAYSIZE(opaque->produce_error_last_time_mutex);
	     ++i) {
		pthread_mutex_init(&opaque->produce_error_last_time_mutex[i],
				   NULL);
	}

	/// @TODO move global_config to static allocated buffer
	opaque->zz_config = &global_config.rb;

	return 0;
}

int zz_opaque_reload(json_t *config, void *opaque) {
	/* Do nothing, since this decoder does not save anything per-listener
	   information */
	(void)config;
	(void)opaque;
	return 0;
}

/** Timer event function, that cleans client's consumed.
  @param zz_config config to print stats
  */
static void
zz_decoder_accounting_tick0(struct zz_config *zz_config, int clean) {
	char err[BUFSIZ];
	rdlog(LOG_DEBUG, "Sending client's accounting information");
	struct kafka_message_array *msgs = NULL;
	struct itimerspec monitor_ts_value, clean_ts_value;

	pthread_rwlock_rdlock(&zz_config->database.rwlock);

	const int get_interval_rc = rb_timer_get_interval(
			zz_config->organizations_sync.timer, &monitor_ts_value);
	if (0 != get_interval_rc) {
		rdlog(LOG_ERR,
		      "Couldn't get timer interval: %s",
		      mystrerror(errno, err, sizeof(err)));
	}
	const int get_clean_interval_rc = rb_timer_get_interval(
			zz_config->organizations_sync.clean_timer,
			&clean_ts_value);
	if (0 != get_clean_interval_rc) {
		rdlog(LOG_ERR,
		      "Couldn't get clean timer interval: %s",
		      mystrerror(errno, err, sizeof(err)));
	}

	if (0 == zz_config->organizations_sync.topics.count) {
		/* No sense to do the processing */
		pthread_rwlock_unlock(&zz_config->database.rwlock);
		return;
	}

	time_t now = time(NULL);
	msgs = organization_db_interval_consumed0(
			&zz_config->database.organizations_db,
			now,
			&monitor_ts_value,
			&clean_ts_value,
			global_config.n2kafka_id,
			clean);

	if (msgs) {
		send_array_to_kafka_topics(
				&zz_config->organizations_sync.topics, msgs);
	}
	pthread_rwlock_unlock(&zz_config->database.rwlock);

	if (msgs) {
		free(msgs);
	}
}

static void zz_decoder_timer_tick(void *ctx, int clean) {
	struct zz_config *zz_cfg = ctx;
	assert_zz_config(zz_cfg);
	zz_decoder_accounting_tick0(zz_cfg, clean);
}

// Convenience function
static void zz_decoder_accounting_tick(void *ctx) {
	const int clean = 0;
	zz_decoder_timer_tick(ctx, clean);
}

static void zz_decoder_org_clean_tick(void *ctx) {
	const int clean = 1;
	zz_decoder_timer_tick(ctx, clean);
}

/** De-register timer if it exists */
static void zz_decoder_deregister_timers(struct zz_config *zz_config) {
	assert(zz_config);

	if (zz_config->organizations_sync.timer) {
		decoder_deregister_timer(zz_config->organizations_sync.timer);
		zz_config->organizations_sync.timer = NULL;
	}

	if (zz_config->organizations_sync.clean_timer) {
		decoder_deregister_timer(
				zz_config->organizations_sync.clean_timer);
		zz_config->organizations_sync.clean_timer = NULL;
	}
}

/** Register organization db sync and organization db clean timers
  @param zz_config config
  @return 0 if success, !0 in other case. Error message is displayed
  */
static int zz_decoder_register_timers(struct zz_config *zz_config) {
	assert(zz_config);
	struct itimerspec my_zero_itimerspec;
	memset(&my_zero_itimerspec, 0, sizeof(my_zero_itimerspec));

	zz_config->organizations_sync.timer =
			decoder_register_timer(&my_zero_itimerspec,
					       zz_decoder_accounting_tick,
					       zz_config);

	if (NULL == zz_config->organizations_sync.timer) {
		rdlog(LOG_ERR, "Couldn't create sync timer (out of memory?)");
		return -1;
	}

	zz_config->organizations_sync.clean_timer =
			decoder_register_timer(&my_zero_itimerspec,
					       zz_decoder_org_clean_tick,
					       zz_config);

	if (NULL == zz_config->organizations_sync.clean_timer) {
		rdlog(LOG_ERR,
		      "Couldn't register clean timer (out of memory?)");
		zz_decoder_deregister_timers(zz_config);
		return -1;
	}

	return 0;
}

/** Actual reload of timer
  @param zz_config redBorder configuration
  @param new_timespec new timer interval
  @todo errors treatment
  */
static void
zz_reload_monitor_timer(struct zz_config *zz_config,
			const struct itimerspec *org_sync_timespec,
			const struct itimerspec *org_clean_timespec) {
	decoder_timer_set_interval(zz_config->organizations_sync.timer,
				   org_sync_timespec);
	decoder_timer_set_interval0(zz_config->organizations_sync.clean_timer,
				    TIMER_ABSTIME,
				    org_clean_timespec);
}

/** Creates a topic handler for organization monitor
  @param monitor_topics_array Monitor topics array
  @return New topic handler
*/
static int create_organization_monitor_topic(json_t *monitor_topics_array,
					     struct rkt_array *rkt_array) {
	size_t array_index;
	json_t *value;

	const size_t n_topics = json_array_size(monitor_topics_array);

	assert(monitor_topics_array);
	assert(n_topics > 0);

	rkt_array->rkt = calloc(n_topics, sizeof(rkt_array->rkt[0]));
	if (NULL == rkt_array->rkt) {
		rdlog(LOG_ERR, "Couldn't allocate topics array");
		return -1;
	}

	rkt_array->count = 0;
	json_array_foreach(monitor_topics_array, array_index, value) {
		char err[BUFSIZ];

		const char *topic_name = json_string_value(value);
		if (NULL == topic_name) {
			rdlog(LOG_ERR,
			      "Couldn't extract monitor topic %zu",
			      array_index);
			continue;
		}

		rkt_array->rkt[rkt_array->count] = new_rkt_global_config(
				topic_name, NULL, err, sizeof(err));
		if (NULL == rkt_array->rkt[rkt_array->count]) {
			rdlog(LOG_ERR,
			      "Couldn't create monitor %s topic: %s",
			      topic_name,
			      err);
			continue;
		}

		rkt_array->count++;
	}

	return 0;
}

/** Format new monitor itimerspec
  @param interval Desired timestamp
  @param monitor_interval itimerspec to format
  @return 0 if success, 1 in other case
  */
static int set_monitor_itimerspec(const time_t interval,
				  struct itimerspec *monitor_interval) {
	memset(monitor_interval, 0, sizeof(*monitor_interval));
	monitor_interval->it_interval.tv_sec =
			monitor_interval->it_value.tv_sec = interval;

	return 0;
}

static void
parse_organization_monitor_sync_clean(struct itimerspec *clean_timerspec,
				      json_int_t clean_mod,
				      json_int_t clean_offset) {

	if (clean_mod == 0) {
		/* Nothing to do */
		return;
	}

	clean_timerspec->it_value.tv_nsec =
			clean_timerspec->it_interval.tv_nsec = 0;
	clean_timerspec->it_interval.tv_sec = clean_mod;

	const time_t now = time(NULL);
	/* previous clean timestamp */
	time_t prev_clean = (now - now % clean_mod) + clean_offset;
	while (prev_clean > now) {
		prev_clean -= clean_mod;
	}
	/* First invocation will be an absolute timestamp */
	clean_timerspec->it_value.tv_sec = prev_clean + clean_mod;
}

static int
parse_organization_monitor_sync(json_t *config,
				struct rkt_array *organizations_monitor_topics,
				struct itimerspec *monitor_interval,
				struct itimerspec *clean_timerspec,
				json_int_t *org_clean_offset_s,
				char **http_put_url_copy) {
	json_error_t jerr;
	json_t *monitor_topics = NULL;
	json_int_t monitor_topic_interval_s = 0, org_clean_mod_s = 0;
	const char *http_put_url = NULL;
	*org_clean_offset_s = 0;

	assert(config);
	assert(organizations_monitor_topics);
	assert(monitor_interval);
	assert(clean_timerspec);

	const int json_unpack_rc = json_unpack_ex(
			config,
			&jerr,
			0,
			"{s?{s?o,s?I,s?s,s:{s:I,s?I}}}",
			RB_ORGANIZATIONS_SYNC_KEY,
			RB_ORGANIZATIONS_SYNC_TOPICS_KEY,
			&monitor_topics,
			RB_ORGANIZATIONS_SYNC_INTERVAL_S_KEY,
			&monitor_topic_interval_s,
			RB_ORGANIZATIONS_SYNC_PUT_URL_KEY,
			&http_put_url,
			RB_ORGANIZATIONS_SYNC_CLEAN_KEY,
			RB_ORGANIZATIONS_SYNC_CLEAN_TIMESTAMP_MOD_KEY,
			&org_clean_mod_s,
			RB_ORGANIZATIONS_SYNC_CLEAN_TIMESTAMP_OFFSET_KEY,
			org_clean_offset_s);

	if (0 != json_unpack_rc) {
		rdlog(LOG_ERR,
		      "Couldn't unpack organization monitor config: "
		      "%s",
		      jerr.text);
		return -1;
	}

	if (*org_clean_offset_s > org_clean_mod_s) {
		const json_int_t new_org_clean_offset_s =
				*org_clean_offset_s % org_clean_mod_s;
		rdlog(LOG_WARNING,
		      "Organization clean offset is bigger than clean mod"
		      " (%lld > %lld). Clean offset will be %lld",
		      *org_clean_offset_s,
		      org_clean_mod_s,
		      new_org_clean_offset_s);

		*org_clean_offset_s = new_org_clean_offset_s;
	}

	parse_organization_monitor_sync_clean(
			clean_timerspec, org_clean_mod_s, *org_clean_offset_s);

	if (monitor_topics == NULL && 0 == monitor_topic_interval_s) {
		/* You don't want monitor, nothing to do */
		return 0;
	} else if (monitor_topics && monitor_topic_interval_s) {
		if (!json_is_array(monitor_topics)) {
			rdlog(LOG_ERR,
			      "%s is not an array!",
			      RB_ORGANIZATIONS_SYNC_TOPICS_KEY);
			return -1;
		}

		if (0 == json_array_size(monitor_topics)) {
			/* You don't want monitor, nothing to do */
			return 0;
		}

		if (http_put_url) {
			*http_put_url_copy = strdup(http_put_url);
			if (NULL == *http_put_url_copy) {
				rdlog(LOG_ERR,
				      "Couldn't copy HTTP PUT URL (out of "
				      "memory?)");
			}
		}

		const int create_rkt_rc = create_organization_monitor_topic(
				monitor_topics, organizations_monitor_topics);

		if (0 != create_rkt_rc) {
			return -1;
		}

		return set_monitor_itimerspec(monitor_topic_interval_s,
					      monitor_interval);
	} else {
		const char *param_set =
				monitor_topics ? RB_ORGANIZATIONS_SYNC_KEY
					       : RB_ORGANIZATIONS_SYNC_INTERVAL_S_KEY;
		const char *param_not_set =
				monitor_topics ? RB_ORGANIZATIONS_SYNC_INTERVAL_S_KEY
					       : RB_ORGANIZATIONS_SYNC_KEY;
		rdlog(LOG_ERR,
		      "You have set %s but not %s,"
		      " no organization monitor will be applied",
		      param_set,
		      param_not_set);
		return -1;
	}
}

int zz_decoder_reload(void *vzz_config, const json_t *config) {
	int rc = 0;
	struct zz_config *zz_config = vzz_config;
	struct topics_db *topics_db = NULL;
	struct rkt_array rkt_array;
	char *http_put_url = NULL;
	struct itimerspec organizations_monitor_topic_ts,
			organizations_clean_ts;
	json_int_t organization_clean_s_offset = 0;
	sensors_db_t *sensors_db = NULL;

	assert(zz_config);
	assert_zz_config(zz_config);
	assert(config);

	memset(&rkt_array, 0, sizeof(rkt_array));
	memset(&organizations_monitor_topic_ts,
	       0,
	       sizeof(organizations_monitor_topic_ts));
	memset(&organizations_clean_ts,
	       0,
	       sizeof(organizations_monitor_topic_ts));
	json_t *my_config = json_deep_copy(config);

	if (my_config == NULL) {
		rdlog(LOG_ERR, "Couldn't deep_copy config (out of memory?)");
		return -1;
	}

	const int organization_sync_rc = parse_organization_monitor_sync(
			my_config,
			&rkt_array,
			&organizations_monitor_topic_ts,
			&organizations_clean_ts,
			&organization_clean_s_offset,
			&http_put_url);

	if (0 != organization_sync_rc) {
		/* Warning already given */
		goto err;
	}

	topics_db = topics_db_new();

	const int topic_list_rc = parse_topic_list_config(my_config, topics_db);
	if (topic_list_rc != 0) {
		rc = -1;
		goto err;
	}

	json_t *organization_uuid =
			json_object_get(my_config, RB_ORGANIZATIONS_UUID_KEY);

	update_sync_topic(&zz_config->organizations_sync.thread,
			  rkt_array.count > 0 ? rkt_array.rkt[0] : NULL);

	pthread_rwlock_wrlock(&zz_config->database.rwlock);
	if (organization_uuid) {
		organizations_db_reload(&zz_config->database.organizations_db,
					organization_uuid);
	}
	sensors_db = parse_per_uuid_opaque_config(
			my_config, &zz_config->database.organizations_db);
	if (NULL != sensors_db) {
		swap_ptrs(sensors_db, zz_config->database.sensors_db);
	} else {
		rc = -1;
	}
	update_sync_thread_clean_interval(
			&zz_config->organizations_sync.thread,
			organizations_clean_ts.it_interval.tv_sec,
			organization_clean_s_offset);
	zz_reload_monitor_timer(zz_config,
				&organizations_monitor_topic_ts,
				&organizations_clean_ts);
	swap_ptrs(topics_db, zz_config->database.topics_db);
	swap_ptrs(zz_config->organizations_sync.http.url, http_put_url);
	rkt_array_swap(&zz_config->organizations_sync.topics, &rkt_array);
	pthread_rwlock_unlock(&zz_config->database.rwlock);

err:
	rkt_array_done(&rkt_array);

	if (topics_db) {
		topics_db_done(topics_db);
	}

	if (sensors_db) {
		sensors_db_destroy(sensors_db);
	}

	if (http_put_url) {
		free(http_put_url);
	}

	json_decref(my_config);

	return rc;
}

void zz_opaque_done(void *_opaque) {
	assert(_opaque);

	struct zz_opaque *opaque = _opaque;
	assert_zz_opaque(opaque);

	free(opaque);
}

/// Elements to format HTTP PUT URL
struct format_http_put_url_ctx {
	/// Base url
	const char *base_url;
	/// Organization uuid
	const char *organization_uuid;
};

/// snprintf convenience function
static int format_http_put_url(char *buf,
			       size_t buf_size,
			       const struct format_http_put_url_ctx *ctx) {
	return snprintf(buf,
			buf_size,
			"%s/%s/reach_bytes_limit",
			ctx->base_url,
			ctx->organization_uuid);
}

static void org_db_limit_reached_http(const organizations_db_t *org_db,
				      const organization_db_entry_t *org,
				      const char *base_url,
				      zz_http2k_curl_handler_t *curl_handler) {
	char err[BUFSIZ];
	const struct format_http_put_url_ctx ctx = {
			.base_url = base_url,
			.organization_uuid =
					organization_db_entry_get_uuid(org),
	};

	(void)org_db;

	const int actual_url_size = format_http_put_url(NULL, 0, &ctx);
	if (actual_url_size < 0) {
		rdlog(LOG_ERR,
		      "Couldn't calculate string size: %s",
		      mystrerror(errno, err, sizeof(err)));
		return;
	}

	char actual_url[actual_url_size + 1];
	const int print_rc = format_http_put_url(
			actual_url, (size_t)actual_url_size + 1, &ctx);

	if (print_rc < 0) {
		rdlog(LOG_ERR,
		      "Couldn't print URL: %s",
		      mystrerror(errno, err, sizeof(err)));
		return;
	} else if (print_rc > actual_url_size) {
		rdlog(LOG_ERR,
		      "Not enough space to print: Needed %d, "
		      "provided %d",
		      print_rc,
		      actual_url_size);
		return;
	}

	zz_http2k_curl_handler_put_empty(curl_handler, actual_url);
}

static void org_db_limit_reached(const organizations_db_t *org_db,
				 const organization_db_entry_t *org,
				 void *ctx) {
	struct zz_config *zz_config = ctx;

	if (zz_config->organizations_sync.http.url) {
		org_db_limit_reached_http(
				org_db,
				org,
				zz_config->organizations_sync.http.url,
				&zz_config->organizations_sync.http
						 .curl_handler);
	}
}

int parse_zz_config(void *vconfig, const struct json_t *config) {
	char err[BUFSIZ];
	struct zz_config *zz_config = vconfig;

	assert(vconfig);
	assert(config);

	if (only_stdout_output()) {
		rdlog(LOG_ERR,
		      "Can't use zz_http2k decoder if not kafka "
		      "brokers configured.");
		return -1;
	}

	const int init_timers_rc = zz_decoder_register_timers(zz_config);
	if (0 != init_timers_rc) {
		return -1;
	}

	rd_kafka_conf_t *rk_conf = rd_kafka_conf_dup(global_config.kafka_conf);
	if (NULL == rk_conf) {
		rdlog(LOG_CRIT, "Couldn't dup conf (out of memory?)");
		goto rk_conf_dup_err;
	}

	const rd_kafka_conf_res_t set_group_id_rc =
			rd_kafka_conf_set(rk_conf,
					  "group.id",
					  "global_config.n2kafka_id",
					  err,
					  sizeof(err));
	if (set_group_id_rc != RD_KAFKA_CONF_OK) {
		rdlog(LOG_ERR,
		      "Couldn't set group.id: %s",
		      rd_kafka_err2str(set_group_id_rc));
		goto rk_conf_err;
	}

	const int sync_thread_init_rc =
			sync_thread_init(&zz_config->organizations_sync.thread,
					 rk_conf,
					 &zz_config->database.organizations_db);
	if (0 != sync_thread_init_rc) {
		goto consumer_thread_err;
	}

	const int init_curl_rc = zz_http2k_curl_handler_init(
			&zz_config->organizations_sync.http.curl_handler,
			10000);
	if (0 != init_curl_rc) {
		goto curl_init_err;
	}

#ifdef ZZ_CONFIG_MAGIC
	zz_config->magic = ZZ_CONFIG_MAGIC;
#endif // ZZ_CONFIG_MAGIC
	const int init_db_rc = init_zz_database(&zz_config->database);
	if (init_db_rc != 0) {
		rdlog(LOG_ERR, "Couldn't init zz_database");
		return -1;
	}

	/// @todo improve this initialization
	zz_config->database.organizations_db.limit_reached_cb =
			org_db_limit_reached;
	zz_config->database.organizations_db.limit_reached_cb_ctx = zz_config;

	/// @TODO error treatment
	const int decoder_reload_rc = zz_decoder_reload(zz_config, config);
	if (decoder_reload_rc != 0) {
		goto reload_err;
	}

	return 0;

reload_err:
	free_valid_zz_database(&zz_config->database);
	zz_http2k_curl_handler_done(
			&zz_config->organizations_sync.http.curl_handler);
curl_init_err:
consumer_thread_err:
rk_conf_err:
/// @TODO: Can't say if we have consumed it!
// rd_kafka_conf_destroy(rk_conf);
rk_conf_dup_err:
	zz_decoder_deregister_timers(zz_config);
	return -1;
}

/** Produce a batch of messages
	@param topic Topic handler
	@param msgs Messages to send
	@param len Length of msgs */
static void produce_or_free(struct zz_opaque *opaque,
			    struct topic_s *topic,
			    rd_kafka_message_t *msgs,
			    int len) {
	assert(topic);
	assert(msgs);
	static const time_t alert_threshold = 5 * 60;

	rd_kafka_topic_t *rkt = topics_db_get_rdkafka_topic(topic);

	const int produce_ret = rd_kafka_produce_batch(rkt,
						       RD_KAFKA_PARTITION_UA,
						       RD_KAFKA_MSG_F_FREE,
						       msgs,
						       len);

	if (produce_ret != len) {
		int i;
		for (i = 0; i < len; ++i) {
			if (msgs[i].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
				time_t last_warning_time = 0;
				int warn = 0;
				const size_t last_warning_time_pos =
						kafka_error_to_warning_time_pos(
								msgs[i].err);

				if (last_warning_time_pos <
				    LAST_WARNING_TIME__END) {
					const time_t curr_time = time(NULL);
					pthread_mutex_lock(
							&opaque->produce_error_last_time_mutex
									 [last_warning_time_pos]);
					last_warning_time =
							opaque->produce_error_last_time
									[last_warning_time_pos];
					if (difftime(curr_time,
						     last_warning_time) >
					    alert_threshold) {
						opaque->produce_error_last_time
								[last_warning_time_pos] =
								curr_time;
						warn = 1;
					}
					pthread_mutex_unlock(
							&opaque->produce_error_last_time_mutex
									 [last_warning_time_pos]);
				}

				if (warn) {
					/* If no alert threshold established or
					 * last alert is too old */
					rdlog(LOG_ERR,
					      "Can't produce to topic %s: %s",
					      rd_kafka_topic_name(rkt),
					      rd_kafka_err2str(msgs[i].err));
				}

				free(msgs[i].payload);
			}
		}
	}
}

/*
 *  MAIN ENTRY POINT
 */

static void process_zz_buffer(const char *buffer,
			      size_t bsize,
			      const keyval_list_t *msg_vars,
			      struct zz_opaque *opaque,
			      struct zz_session **sessionp) {

	// json_error_t err;
	// struct zz_database *db = &opaque->zz_config->database;
	// /* @TODO const */ json_t *uuid_enrichment_entry = NULL;
	// char *ret = NULL;
	struct zz_session *session = NULL;
	const unsigned char *in_iterator = (const unsigned char *)buffer;

	assert(sessionp);

	if (NULL == *sessionp) {
		/* First call */
		*sessionp = new_zz_session(opaque->zz_config, msg_vars);
		if (NULL == *sessionp) {
			return;
		}
	} else if (0 == bsize) {
		/* Last call, need to free session */
		free_zz_session(*sessionp);
		*sessionp = NULL;
		return;
	}

	session = *sessionp;

	/* If the client has reached limit, it is not allowed to keep sending
	   bytes
	   */
	organization_db_entry_t *organization =
			sensor_db_entry_organization(session->sensor);
	if (organization && organization_limit_reached(organization)) {
		organization_add_consumed_bytes(organization, bsize);
		return;
	}

	yajl_status stat = yajl_parse(session->handler, in_iterator, bsize);

	if (stat != yajl_status_ok) {
		if (organization && organization_limit_reached(organization)) {
			/* We have stop the parsing because quota, so no
			   need to warn here again */
			/* Adding the rest of message as if it has been
			   consumed. This way, we can get a real account of
			   client's sent bytes.

			   This number will not be exactly the same as if we
			   were consumed the enriched message, but if we want
			   not to parse it, we can't do better */
			const size_t rest_of_message =
					bsize -
					yajl_get_bytes_consumed(
							session->handler);
			organization_add_consumed_bytes(organization,
							rest_of_message);
		} else {
			/// @TODO improve this!
			unsigned char *str = yajl_get_error(session->handler,
							    1,
							    in_iterator,
							    bsize);
			fprintf(stderr, "%s", (const char *)str);
			yajl_free_error(session->handler, str);
		}
	}
}

void zz_decode(char *buffer,
	       size_t buf_size,
	       const keyval_list_t *list,
	       void *_listener_callback_opaque,
	       void **vsessionp) {
	struct zz_opaque *zz_opaque = _listener_callback_opaque;
	struct zz_session **sessionp = (struct zz_session **)vsessionp;
	/// Helper pointer to simulate streaming behavior
	struct zz_session *my_session = NULL;

	assert_zz_opaque(zz_opaque);

	if (NULL == vsessionp) {
		// Simulate an active
		sessionp = &my_session;
	}

	process_zz_buffer(buffer, buf_size, list, zz_opaque, sessionp);

	if (buffer) {
		/* It was not the last call, designed to free session */
		const size_t n_messages =
				rd_kafka_msg_q_size(&(*sessionp)->msg_queue);
		rd_kafka_message_t msgs[n_messages];
		rd_kafka_msg_q_dump(&(*sessionp)->msg_queue, msgs);

		if ((*sessionp)->topic) {
			produce_or_free(zz_opaque,
					(*sessionp)->topic_handler,
					msgs,
					n_messages);
		}
	}

	if (NULL == vsessionp) {
		// Simulate last call that will free my_session
		process_zz_buffer(NULL, 0, list, zz_opaque, sessionp);
	}
}

void zz_decoder_done(void *vzz_config) {
	struct zz_config *zz_config = vzz_config;
	assert_zz_config(zz_config);
	zz_decoder_deregister_timers(zz_config);
	zz_http2k_curl_handler_done(
			&zz_config->organizations_sync.http.curl_handler);

	rkt_array_done(&zz_config->organizations_sync.topics);

	sync_thread_done(&zz_config->organizations_sync.thread);

	free_valid_zz_database(&zz_config->database);
}
