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

#include "rb_meraki_tests.h"

static const char MERAKI_MSG[] =
		// *INDENT-OFF*
		"{"
		"\"version\":\"2.0\","
		"\"secret\":\"r3dB0rder\","
		"\"type\":\"DevicesSeen\","
		"\"data\":{"
		"\"apMac\":\"55:55:55:55:55:55\","
		"\"apFloors\":[],"
		"\"apTags\":[],"
		"\"observations\":["
		"{"
		"\"ipv4\":\"/10.1.3.38\","
		"\"location\":{"
		"\"lat\":37.42205275787813,"
		"\"lng\":-122.20766382990405,"
		"\"unc\":49.0,"
		"\"x\":["
		"],"
		"\"y\":["
		"]"
		"},"
		"\"seenTime\":\"2015-05-19T07:30:34Z\","
		"\"ssid\":\"Trinity\","
		"\"os\":\"Apple iOS\","
		"\"clientMac\":\"78:3a:84:11:22:33\","
		"\"seenEpoch\":1432020634,"
		"\"rssi\":0,"
		"\"ipv6\":null,"
		"\"manufacturer\":\"Apple\""
		"},"
		"{"
		"\"ipv4\":null,"
		"\"location\":{"
		"\"lat\":37.42200897584358,"
		"\"lng\":-122.20751219778322,"
		"\"unc\":23.641346501668412,"
		"\"x\":["
		"],"
		"\"y\":["
		"]"
		"},"
		"\"seenTime\":\"2015-05-19T07:30:30Z\","
		"\"ssid\":null,"
		"\"os\":null,"
		"\"clientMac\":\"80:56:f2:44:55:66\","
		"\"seenEpoch\":1432020630,"
		"\"rssi\":13,"
		"\"ipv6\":null,"
		"\"manufacturer\":\"Hon Hai/Foxconn\""
		"},"
		"{"
		"\"ipv4\":\"/10.1.3.41\","
		"\"location\":{"
		"\"lat\":37.42205737322192,"
		"\"lng\":-122.20762896118686,"
		"\"unc\":37.49420236988837,"
		"\"x\":["
		"],"
		"\"y\":["
		"]"
		"},"
		"\"seenTime\":\"2015-05-19T07:30:34Z\","
		"\"ssid\":\"Trinity\","
		"\"os\":\"Apple iOS\","
		"\"clientMac\":\"3c:ab:8e:77:88:99\","
		"\"seenEpoch\":1432020634,"
		"\"rssi\":0,"
		"\"ipv6\":null,"
		"\"manufacturer\":\"Apple\""
		"}"
		"]"
		"}"
		"}";
// *INDENT-ON*

const char MERAKI_EMPTY_OBSERVATIONS_MSG[] =
		// *INDENT-OFF*
		"{"
		"\"version\":\"2.0\","
		"\"secret\":\"r3dB0rder\","
		"\"type\":\"DevicesSeen\","
		"\"data\":{"
		"\"apMac\":\"55:55:55:55:55:55\","
		"\"apFloors\":[],"
		"\"apTags\":[],"
		"\"observations\":[]"
		"}"
		"}";
// *INDENT-ON*

static const char MERAKI_SECRETS_IN[] = // *INDENT-OFF*
		"{"
		/* "\"meraki-secrets\": {" */
		"\"r3dB0rder\": { "
		"\"sensor_name\": \"meraki1\" "
		", \"sensor_id\": 2"
		"},"
		"\"r3dB0rder2\": { "
		"\"sensor_name\": \"meraki2\" "
		", \"sensor_id\": 3"
		"}"
		/* "}" */
		"}";
// *INDENT-ON*

static const char MERAKI_SECRETS_DEFAULT_IN[] = // *INDENT-OFF*
		"{"
		/* "\"meraki-secrets\": {" */
		"\"r3dB0rder\": { "
		"\"sensor_name\": \"meraki1\" "
		", \"sensor_id\": 2"
		"},"
		"\"*\": { "
		"\"sensor_name\": \"default\" "
		", \"sensor_id\": 3"
		"}"
		/* "}" */
		"}";
// *INDENT-ON*

static const char MERAKI_SECRETS_OUT[] = // *INDENT-OFF*
		"{"
		/* "\"meraki-secrets\": {" */
		"\"r3dB0rder3\": { "
		"\"sensor_name\": \"meraki1\" "
		", \"sensor_id\": 2"
		"},"
		"\"r3dB0rder2\": { "
		"\"sensor_name\": \"meraki2\" "
		", \"sensor_id\": 3"
		"}"
		/* "}" */
		"}";
// *INDENT-ON*

static const char MERAKI_SECRETS_DEFAULT_OUT[] = // *INDENT-OFF*
		"{"
		/* "\"meraki-secrets\": {" */
		"\"r3dB0rder3\": { "
		"\"sensor_name\": \"meraki1\" "
		", \"sensor_id\": 2"
		"},"
		"\"*\": { "
		"\"sensor_name\": \"default\" "
		", \"sensor_id\": 3"
		"}"
		/* "}" */
		"}";
// *INDENT-ON*

CHECKDATA(check1,
	  {.key = "type", .value = "meraki"},
	  {.key = "wireless_station", .value = "55:55:55:55:55:55"},
	  {.key = "src", .value = "10.1.3.38"},
	  {.key = "client_os", .value = "Apple iOS"},
	  {.key = "client_mac_vendor", .value = "Apple"},
	  {.key = "client_mac", .value = "78:3a:84:11:22:33"},
	  {.key = "timestamp", .value = "1432020634"},
	  {.key = "client_rssi_num", .value = "-95"},
	  {.key = "client_latlong", .value = "37.42205,-122.20766"},
	  {.key = "wireless_id", .value = "Trinity"});

CHECKDATA(check2,
	  {.key = "type", .value = "meraki"},
	  {.key = "wireless_station", .value = "55:55:55:55:55:55"},
	  {.key = "src", .value = NULL},
	  {.key = "client_os", .value = NULL},
	  {.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
	  {.key = "client_mac", .value = "80:56:f2:44:55:66"},
	  {.key = "timestamp", .value = "1432020630"},
	  {.key = "client_rssi_num", .value = "-82"},
	  {.key = "client_latlong", .value = "37.42201,-122.20751"},
	  {.key = "wireless_id", .value = NULL});

CHECKDATA(check3,
	  {.key = "type", .value = "meraki"},
	  {.key = "wireless_station", .value = "55:55:55:55:55:55"},
	  {.key = "src", .value = "10.1.3.41"},
	  {.key = "client_os", .value = "Apple iOS"},
	  {.key = "client_mac_vendor", .value = "Apple"},
	  {.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
	  {.key = "timestamp", .value = "1432020634"},
	  {.key = "client_rssi_num", .value = "-95"},
	  {.key = "client_latlong", .value = "37.42206,-122.20763"},
	  {.key = "wireless_id", .value = "Trinity"});

static void MerakiDecoder_valid_enrich() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_base(
			NULL, MERAKI_SECRETS_IN, MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_novalid_enrich() {
	struct checkdata_array *checkdata = NULL;
	MerakiDecoder_test_base(
			NULL, MERAKI_SECRETS_OUT, MERAKI_MSG, checkdata);
}

static void MerakiDecoder_empty_observations() {
	struct checkdata_array *checkdata = NULL;
	MerakiDecoder_test_base(NULL,
				MERAKI_SECRETS_IN,
				MERAKI_EMPTY_OBSERVATIONS_MSG,
				checkdata);
}

CHECKDATA(check1_listener_enrich,
	  {.key = "type", .value = "meraki"},
	  {.key = "wireless_station", .value = "55:55:55:55:55:55"},
	  {.key = "src", .value = "10.1.3.38"},
	  {.key = "client_os", .value = "Apple iOS"},
	  {.key = "client_mac_vendor", .value = "Apple"},
	  {.key = "client_mac", .value = "78:3a:84:11:22:33"},
	  {.key = "timestamp", .value = "1432020634"},
	  {.key = "client_rssi_num", .value = "-95"},
	  {.key = "client_latlong", .value = "37.42205,-122.20766"},
	  {.key = "wireless_id", .value = "Trinity"},
	  {.key = "a", .value = "1"},
	  {.key = "b", .value = "c"});

CHECKDATA(check2_listener_enrich,
	  {.key = "type", .value = "meraki"},
	  {.key = "wireless_station", .value = "55:55:55:55:55:55"},
	  {.key = "src", .value = NULL},
	  {.key = "client_os", .value = NULL},
	  {.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
	  {.key = "client_mac", .value = "80:56:f2:44:55:66"},
	  {.key = "timestamp", .value = "1432020630"},
	  {.key = "client_rssi_num", .value = "-82"},
	  {.key = "client_latlong", .value = "37.42201,-122.20751"},
	  {.key = "wireless_id", .value = NULL},
	  {.key = "a", .value = "1"},
	  {.key = "b", .value = "c"});

CHECKDATA(check3_listener_enrich,
	  {.key = "type", .value = "meraki"},
	  {.key = "wireless_station", .value = "55:55:55:55:55:55"},
	  {.key = "src", .value = "10.1.3.41"},
	  {.key = "client_os", .value = "Apple iOS"},
	  {.key = "client_mac_vendor", .value = "Apple"},
	  {.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
	  {.key = "timestamp", .value = "1432020634"},
	  {.key = "client_rssi_num", .value = "-95"},
	  {.key = "client_latlong", .value = "37.42206,-122.20763"},
	  {.key = "wireless_id", .value = "Trinity"},
	  {.key = "a", .value = "1"},
	  {.key = "b", .value = "c"});

static void MerakiDecoder_valid_enrich_per_listener() {
	CHECKDATA_ARRAY(checkdata,
			&check1_listener_enrich,
			&check2_listener_enrich,
			&check3_listener_enrich);

	MerakiDecoder_test_base("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}",
				MERAKI_SECRETS_IN,
				MERAKI_MSG,
				&checkdata);
}

CHECKDATA(check_default1,
	  {.key = "type", .value = "meraki"},
	  {.key = "wireless_station", .value = "55:55:55:55:55:55"},
	  {.key = "src", .value = "10.1.3.38"},
	  {.key = "client_os", .value = "Apple iOS"},
	  {.key = "client_mac_vendor", .value = "Apple"},
	  {.key = "client_mac", .value = "78:3a:84:11:22:33"},
	  {.key = "timestamp", .value = "1432020634"},
	  {.key = "client_rssi_num", .value = "-95"},
	  {.key = "client_latlong", .value = "37.42205,-122.20766"},
	  {.key = "wireless_id", .value = "Trinity"},
	  {.key = "sensor_name", .value = "default"});

CHECKDATA(check_default2,
	  {.key = "type", .value = "meraki"},
	  {.key = "wireless_station", .value = "55:55:55:55:55:55"},
	  {.key = "src", .value = NULL},
	  {.key = "client_os", .value = NULL},
	  {.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
	  {.key = "client_mac", .value = "80:56:f2:44:55:66"},
	  {.key = "timestamp", .value = "1432020630"},
	  {.key = "client_rssi_num", .value = "-82"},
	  {.key = "client_latlong", .value = "37.42201,-122.20751"},
	  {.key = "wireless_id", .value = NULL},
	  {.key = "sensor_name", .value = "default"});

CHECKDATA(check_default3,
	  {.key = "type", .value = "meraki"},
	  {.key = "wireless_station", .value = "55:55:55:55:55:55"},
	  {.key = "src", .value = "10.1.3.41"},
	  {.key = "client_os", .value = "Apple iOS"},
	  {.key = "client_mac_vendor", .value = "Apple"},
	  {.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
	  {.key = "timestamp", .value = "1432020634"},
	  {.key = "client_rssi_num", .value = "-95"},
	  {.key = "client_latlong", .value = "37.42206,-122.20763"},
	  {.key = "wireless_id", .value = "Trinity"},
	  {.key = "sensor_name", .value = "default"});

static void MerakiDecoder_default_secret_hit() {
	CHECKDATA_ARRAY(checkdata,
			&check1_listener_enrich,
			&check2_listener_enrich,
			&check3_listener_enrich);

	MerakiDecoder_test_base("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}",
				MERAKI_SECRETS_DEFAULT_IN,
				MERAKI_MSG,
				&checkdata);
}

static void MerakiDecoder_default_secret_miss() {
	CHECKDATA_ARRAY(checkdata,
			&check_default1,
			&check_default2,
			&check_default3);

	MerakiDecoder_test_base("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}",
				MERAKI_SECRETS_DEFAULT_OUT,
				MERAKI_MSG,
				&checkdata);
}

int main() {
	const struct CMUnitTest tests[] = {
			cmocka_unit_test(MerakiDecoder_valid_enrich),
			cmocka_unit_test(MerakiDecoder_novalid_enrich),
			cmocka_unit_test(
					MerakiDecoder_valid_enrich_per_listener),
			cmocka_unit_test(MerakiDecoder_empty_observations),
			cmocka_unit_test(MerakiDecoder_default_secret_hit),
			cmocka_unit_test(MerakiDecoder_default_secret_miss)};

	return cmocka_run_group_tests(tests, NULL, meraki_global_done);
}
