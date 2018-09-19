/*
** Copyright (C) 2014-2016, Eneo Tecnologia S.L.
** Copyright (C) 2017, Eugenio Perez <eupm90@gmail.com>
** Copyright (C) 2018, Wizzie S.L.
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

#include "config.h"
#include "engine.h"
#include "global_config.h"
#include "util/kafka.h"

#include <jansson.h>
#include <librd/rd.h>

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int do_reload = 0;

static void shutdown_process() {
	printf("Exiting\n");
	do_shutdown = 1;
}

static void show_usage(const char *progname) {
	fprintf(stdout, "n2kafka version %s\n", GITVERSION);
	fprintf(stdout, "Usage: %s <config_file>\n", progname);
	fprintf(stdout, "\n");
	fprintf(stdout,
		"Where <config_file> is a json file that can contains the \n");
	fprintf(stdout, "the next configurations:\n");

	fprintf(stdout, "{\n");
	fprintf(stdout, "\t\"listeners:\":[\n");
	fprintf(stdout,
		"\t\t{\"proto\":\"http\",\"port\":2057,\"mode\":\"(1)\","
		"\"threads\":20}\n");
	fprintf(stdout,
		"\t\t{\"proto\":\"tcp\",\"port\":2056,"
		"\"tcp_leepalive\":true,\"mode\"},\n");
	fprintf(stdout,
		"\t\t{\"proto\":\"udp\",\"port\":2058,\"threads\":20}\n");
	fprintf(stdout, "\t],\n");
	fprintf(stdout, "\t\"brokers\":\"kafka brokers\",\n");
	fprintf(stdout, "\t\"topic\":\"kafka topic\",\n");
	fprintf(stdout, "\t\"rdkafka.socket.max.fails\":\"3\",\n");
	fprintf(stdout, "\t\"rdkafka.socket.keepalive.enable\":\"true\",\n");
	fprintf(stdout, "\t\"blacklist\":[\"192.168.101.3\"]\n");
	fprintf(stdout, "}\n\n");
	fprintf(stdout, "(1) Modes can be:\n");
	fprintf(stdout, "\tthread_per_connection: Creates a thread for each "
			"connection.\n");
	fprintf(stdout, "\t\tThread argument will be ignored in this mode\n");
	fprintf(stdout,
		"\tselect,poll,epoll: Fixed number of threads (with threads "
		"parameter) manages all connections\n");
}

static int is_asking_help(const char *param) {
	return 0 == strcmp(param, "-h") || 0 == strcmp(param, "--help");
}

static void sighup_proc(int signum __attribute__((unused))) {
	do_reload = 1;
}

static sigset_t signal_setv(size_t n, const int *signals) {
	sigset_t ret;
	sigemptyset(&ret);

	for (size_t i = 0; i < n; ++i) {
		sigaddset(&ret, signals[i]);
	}

	return ret;
}

int main(int argc, char *argv[]) {
	static const int SIGNALS[] = {SIGINT, SIGHUP};
	const sigset_t sig_block = signal_setv(RD_ARRAYSIZE(SIGNALS), SIGNALS);
	sigset_t old_sigset;

	if (argc != 2 || is_asking_help(argv[1])) {
		show_usage(argv[0]);
		exit(1);
	}

	// Make signal handling only in main thread
	sigprocmask(SIG_BLOCK, &sig_block, &old_sigset);

	init_global_config();
	parse_config(argv[1]);

	signal(SIGINT, shutdown_process);
	signal(SIGHUP, sighup_proc);

	// Restore signal processing
	sigprocmask(SIG_SETMASK, &old_sigset, NULL);

	while (!do_shutdown) {
		kafka_poll(1000 /* ms */);
		if (do_reload) {
			reload_config(&global_config);
			do_reload = 0;
		}
	}

	free_global_config();

	return 0;
}
