/*
** Copyright (C) 2014-2016, Eneo Tecnologia S.L.
** Copyright (C) 2017, Eugenio Perez <eupm90@gmail.com>
** Copyright (C) 2018-2019, Wizzie S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
**
** This file is part of n2kafka.
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

#include "socket.h"

#include "engine/global_config.h"
#include "engine/rb_addr.h"
#include "util/in_addr_list.h"
#include "util/pair.h"
#include "util/util.h"

#include <ev.h>
#include <jansson.h>
#include <librd/rdlog.h>
#include <librd/rdthread.h>

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

struct n2k_decoder;

#define MAX_NUM_THREADS 256

#define CONNECTION_PRIVATE_MAGIC 0x45235612L

#define N2KAFKA_TCP "tcp"
#define N2KAFKA_UDP "udp"

enum thread_mode {
#define STR_MODE_THREAD_PER_CONNECTION "thread_per_connection"
	MODE_THREAD_PER_CONNECTION,
#define STR_MODE_SELECT "select"
	MODE_SELECT,
#define STR_MODE_POLL "poll"
	MODE_POLL,
#define STR_MODE_EPOLL "epoll"
	MODE_EPOLL,
	MODE_INVALID
};

struct udp_thread_info {
	pthread_mutex_t listenfd_mutex;
	int listenfd;
	const struct listener *listener;
};

static enum thread_mode thread_mode_str(const char *mode_str) {
	if (NULL == mode_str ||
	    0 == strcmp(STR_MODE_THREAD_PER_CONNECTION, mode_str))
		return MODE_THREAD_PER_CONNECTION;
	if (0 == strcmp(STR_MODE_SELECT, mode_str))
		return MODE_SELECT;
	if (0 == strcmp(STR_MODE_POLL, mode_str))
		return MODE_POLL;
	if (0 == strcmp(STR_MODE_EPOLL, mode_str))
		return MODE_EPOLL;
	return MODE_INVALID;
}

#define READ_BUFFER_SIZE 4096
static const struct timeval WRITE_SELECT_TIMEVAL = {.tv_sec = 5, .tv_usec = 0};

/// @TODO this can't be global, it produces a race condition!
static int do_shutdown = 0;

static int createListenSocket(const char *proto, uint16_t listen_port) {
	int listenfd = 0;
	if (NULL == proto) {
		rdlog(LOG_ERR, "Can't create listen socket: No protocol given");
		return 0;
	}

	if (0 == strcmp(N2KAFKA_UDP, proto)) {
		listenfd = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
	} else if (0 == strcmp(N2KAFKA_TCP, proto)) {
		listenfd = socket(AF_INET, SOCK_STREAM, 0);
	} else {
		rdlog(LOG_ERR, "Can't create socket: Unknown type");
		return 0;
	}

	if (listenfd == -1) {
		rdlog(LOG_ERR,
		      "Error creating socket: %s",
		      gnu_strerror_r(errno));
		return 0;
	}

	const int so_reuseaddr_value = 1;
	const int setsockopt_ret = setsockopt(listenfd,
					      SOL_SOCKET,
					      SO_REUSEADDR,
					      &so_reuseaddr_value,
					      sizeof(so_reuseaddr_value));
	if (setsockopt_ret < 0) {
		rdlog(LOG_WARNING,
		      "Error setting socket option: %s",
		      gnu_strerror_r(errno));
	}

	struct sockaddr_in server_addr;
	memset(&server_addr, 0, sizeof(server_addr));

	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	assert(listen_port > 0);
	server_addr.sin_port = htons(listen_port);

	const int bind_ret = bind(listenfd,
				  (struct sockaddr *)&server_addr,
				  sizeof(server_addr));
	if (bind_ret == -1) {
		rdlog(LOG_ERR,
		      "Error binding socket: %s",
		      gnu_strerror_r(errno));
		close(listenfd);
		return -1;
	}

	if (0 == strcmp(N2KAFKA_TCP, proto)) {
		const int listen_ret = listen(listenfd, SOMAXCONN);
		if (listen_ret == -1) {
			rdlog(LOG_ERR,
			      "Error in listen: %s",
			      gnu_strerror_r(errno));
			close(listenfd);
			return -1;
		}
	}

	rdlog(LOG_INFO, "Listening socket created successfuly");
	return listenfd;
}

static int createListenSocketMutex(pthread_mutex_t *mutex) {
	const int init_returned = pthread_mutex_init(mutex, NULL);
	if (init_returned != 0)
		rdlog(LOG_ERR, "Error creating mutex: ");
	return init_returned;
}

static void set_nonblock_flag(int fd) {
	int flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void set_keepalive_opt(int fd) {
	int i = 1;
	const int sso_rc = setsockopt(
			fd, SOL_SOCKET, SO_KEEPALIVE, (char *)&i, sizeof(int));
	if (sso_rc == -1)
		rdbg("Can't set SO_KEEPALIVE option");
}

static uint16_t get_port(const struct sockaddr_in *sa) {
	return ntohs(sa->sin_port);
}

static void print_accepted_connection_log(const struct sockaddr_in *sa) {
	char str[sizeof(INET_ADDRSTRLEN)];
	inet_ntop(AF_INET, &(sa->sin_addr), str, INET_ADDRSTRLEN);

	rdlog(LOG_INFO, "Accepted connection from %s:%d", str, get_port(sa));
}

static int select_socket(int listenfd, struct timeval *tv) {
	fd_set listenfd_set;

	FD_ZERO(&listenfd_set);
	FD_SET(listenfd, &listenfd_set);
	return select(listenfd + 1, &listenfd_set, NULL, NULL, tv);
}

static int write_select_socket(int writefd, struct timeval *tv) {
	fd_set writefd_set;
	FD_ZERO(&writefd_set);
	FD_SET(writefd, &writefd_set);
	return select(writefd + 1, NULL, &writefd_set, NULL, tv);
}

static int receive_from_socket(int fd,
			       struct sockaddr_in6 *addr,
			       char *buffer,
			       const size_t buffer_size) {
	socklen_t socklen = (socklen_t)sizeof(*addr);
	return recvfrom(fd,
			buffer,
			buffer_size,
			MSG_DONTWAIT,
			(struct sockaddr *)addr,
			&socklen);
}

static void process_data_received_from_socket(char *buffer,
					      const size_t recv_result,
					      const char *client,
					      const struct listener *l) {
	rdlog(LOG_DEBUG,
	      "received %zu data from %s: %.*s",
	      recv_result,
	      client,
	      (int)recv_result,
	      buffer);

	struct pair attrs_mem[1];
	attrs_mem->key = "client_ip";
	attrs_mem->value = client;

	keyval_list_t attrs = keyval_list_initializer(attrs);
	add_key_value_pair(&attrs, attrs_mem);

	listener_decode(l, buffer, recv_result, &attrs, NULL, NULL, NULL);
}

static int send_to_socket(int fd, const char *data, size_t len) {
	struct timeval tv = WRITE_SELECT_TIMEVAL;
	const int select_result = write_select_socket(fd, &tv);
	if (select_result > 0) {
		return write(fd, data, len);
	} else if (select_result == 0) {
		rdlog(LOG_ERR,
		      "Socket not ready for writing in %ld.%6ld. Closing.",
		      WRITE_SELECT_TIMEVAL.tv_sec,
		      WRITE_SELECT_TIMEVAL.tv_usec);
		return select_result;
	} else {
		rdlog(LOG_ERR,
		      "Error writing to socket: %s",
		      gnu_strerror_r(errno));
		return select_result;
	}
}

struct connection_private {
#ifdef CONNECTION_PRIVATE_MAGIC
	uint64_t magic;
#endif
	int first_response_sent;
	const struct listener *listener;
	const char *client;
};

static void
close_socket_and_stop_watcher(struct ev_loop *loop, struct ev_io *watcher) {
	ev_io_stop(loop, watcher);

	close(watcher->fd);
	free(watcher);
}

static void read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {

	if (EV_ERROR & revents) {
		rdlog(LOG_ERR,
		      "Read callback error: %s",
		      gnu_strerror_r(errno));
	}

	struct connection_private *connection =
			(struct connection_private *)watcher->data;
	struct sockaddr_in6 saddr;

#ifdef CONNECTION_PRIVATE_MAGIC
	assert(connection->magic == CONNECTION_PRIVATE_MAGIC);
#endif

	char *buffer = calloc(READ_BUFFER_SIZE, sizeof(char));
	const int recv_result = receive_from_socket(
			watcher->fd, &saddr, buffer, READ_BUFFER_SIZE);
	if (recv_result > 0) {
		process_data_received_from_socket(buffer,
						  (size_t)recv_result,
						  connection->client,
						  connection->listener);
	} else if (recv_result < 0) {
		if (errno == EAGAIN) {
			rdbg("Socket not ready. re-trying");
			free(buffer);
			return;
		} else {
			rdlog(LOG_ERR, "Recv error: %s", gnu_strerror_r(errno));
			free(buffer);
			close_socket_and_stop_watcher(loop, watcher);
			return;
		}
	} else { /* recv_result == 0 */
		free(buffer);
		close_socket_and_stop_watcher(loop, watcher);
		return;
	}

	if (NULL != global_config.response &&
	    !connection->first_response_sent) {
		int send_ret = 1;
		rdlog(LOG_DEBUG, "Sending first response...");

		if (global_config.response_len == 0) {
			rdlog(LOG_ERR,
			      "Can't send first response to %s: size of "
			      "response == 0",
			      connection->client);
			connection->first_response_sent = 1;
		} else {
			send_ret = send_to_socket(
					watcher->fd,
					global_config.response,
					(size_t)global_config.response_len - 1);
		}

		if (send_ret <= 0) {
			rdlog(LOG_ERR,
			      "Cannot send first response to %s socket: %s",
			      connection->client,
			      gnu_strerror_r(errno));
			close_socket_and_stop_watcher(loop, watcher);
		}

		rdlog(LOG_DEBUG, "first response ok");
		connection->first_response_sent = 1;
	}
}

#define SOCKET_LISTENER_PRIVATE_MAGIC 0xB0C31331AEA1CL

struct socket_listener {
	struct listener listener;
#ifdef SOCKET_LISTENER_PRIVATE_MAGIC
	uint64_t magic;
#endif
	pthread_t main_loop;
	struct ev_loop *event_loop;
	struct ev_async w_async;

	struct {
		char *proto;
		size_t threads;
		bool tcp_keepalive;
		enum thread_mode thread_mode;
	} config;

	pthread_t threads[MAX_NUM_THREADS];
	struct ev_loop *event_loops[MAX_NUM_THREADS];
	struct ev_async event_asyncs[MAX_NUM_THREADS];
	rd_fifoq_t watchers_queue[MAX_NUM_THREADS];

	size_t accept_current_worker_idx;
};

static void accept_cb(struct ev_loop *loop __attribute__((unused)),
		      struct ev_io *watcher,
		      int revents) {
	struct sockaddr_in client_saddr;
	socklen_t client_len = sizeof(client_saddr);
	struct socket_listener *socket_listener =
			(struct socket_listener *)watcher->data;
	int client_sd;
	char client_buf[BUFSIZ];

	if (EV_ERROR & revents) {
		rdlog(LOG_ERR, "Invalid event: %s", gnu_strerror_r(errno));
		return;
	}

	client_sd = accept(watcher->fd,
			   (struct sockaddr *)&client_saddr,
			   &client_len);

	if (client_sd < 0) {
		rdlog(LOG_ERR, "accept error: %s", gnu_strerror_r(errno));
		return;
	}

	const char *client_addr =
			sockaddr2str(client_buf,
				     sizeof(client_buf),
				     (struct sockaddr *)&client_saddr);
	if (NULL == client_addr) {
		rdlog(LOG_ERR, "couldn't get client address");
		return;
	}
	const size_t client_addr_len = strlen(client_addr);

	if (in_addr_list_contains(global_config.blacklist,
				  &client_saddr.sin_addr)) {
		rdlog(LOG_INFO,
		      "Connection rejected: %s in blacklist",
		      client_addr);
		close(client_sd);
		return;
	}

	print_accepted_connection_log((struct sockaddr_in *)&client_saddr);

	if (socket_listener->config.tcp_keepalive)
		set_keepalive_opt(client_sd);
	set_nonblock_flag(client_sd);

	if (socket_listener->config.thread_mode == MODE_THREAD_PER_CONNECTION) {
		rdlog(LOG_ERR,
		      "Mode " STR_MODE_THREAD_PER_CONNECTION
		      "still not implemented");
		exit(-1);
	} else {
		/* Set watcher. Private data just after watcher */
		struct ev_io *w_client = calloc(
				1,
				sizeof(struct ev_io) +
						sizeof(struct connection_private) +
						client_addr_len + 1);
		if (unlikely(NULL == w_client)) {
			rdlog(LOG_ERR,
			      "Can't allocate client %s private data",
			      client_addr);
		} else {
			struct connection_private *conn_priv = NULL;
			w_client->data = conn_priv =
					(struct connection_private
							 *)&w_client[1];
#if CONNECTION_PRIVATE_MAGIC
			conn_priv->magic = CONNECTION_PRIVATE_MAGIC;
#endif
			conn_priv->listener = &socket_listener->listener;

			const size_t cur_idx =
					socket_listener->accept_current_worker_idx++;
			if (socket_listener->accept_current_worker_idx >=
			    socket_listener->config.threads)
				socket_listener->accept_current_worker_idx = 0;

			conn_priv->client = strncat((char *)&conn_priv[1],
						    client_addr,
						    client_addr_len + 1);

			rdbg("Sent connection of %s to worker thread %zu",
			     client_addr,
			     cur_idx);

			ev_io_init(w_client, read_cb, client_sd, EV_READ);
			rd_fifoq_add(&socket_listener->watchers_queue[cur_idx],
				     w_client);
			ev_async_send(socket_listener->event_loops[cur_idx],
				      &socket_listener->event_asyncs[cur_idx]);
		}
	}
}

struct worker_args {
	struct socket_listener *socket_listener;
	size_t idx;
};

static void async_cb(struct ev_loop *loop,
		     ev_async *w __attribute__((unused)),
		     int revents) {
	struct worker_args *args = ev_userdata(loop);

	if (EV_ERROR & revents) {
		rdlog(LOG_ERR, "Invalid event: %s", gnu_strerror_r(errno));
		return;
	}

	if (1 == do_shutdown) {
		/* The signal was for end the loop */
		ev_break(loop, EVBREAK_ALL);
	}

	if (args) {
		/* The signal was alerting a new socket to watch */
		size_t i = args->idx;
		rd_fifoq_elm_t *qelm = NULL;
		while ((qelm = rd_fifoq_pop(
					&args->socket_listener->watchers_queue
							 [i]))) {
			struct ev_io *w_client = qelm->rfqe_ptr;
			if (NULL != w_client) {
				ev_io_start(loop, w_client);
			}

			rd_fifoq_elm_release(
					&args->socket_listener
							 ->watchers_queue[i],
					qelm);
		}
	}
}

static void *worker(void *_worker_arg) {
	struct worker_args *worker_args = _worker_arg;

	ev_run(worker_args->socket_listener->event_loops[worker_args->idx], 0);

	free(worker_args);

	return NULL;
}

static void
main_tcp_loop(int listenfd, struct socket_listener *socket_listener) {
	socket_listener->event_loop = ev_loop_new(0);
	struct ev_io w_accept = {
			.data = socket_listener,
	};

	if (NULL == socket_listener->event_loop) {
		rdlog(LOG_ERR, "Can't initialize event loop (out of memory?)");
		return;
	}

	ev_io_init((&w_accept), accept_cb, listenfd, EV_READ);
	ev_async_init((&socket_listener->w_async), async_cb);
	ev_io_start(socket_listener->event_loop, &w_accept);
	ev_async_start(socket_listener->event_loop, &socket_listener->w_async);

	size_t i;
	for (i = 0; i < socket_listener->config.threads; ++i) {
		struct worker_args *args = calloc(1, sizeof(args[0]));
		if (!args) {
			rdlog(LOG_ERR,
			      "Can't allocate worker arg (out of memory?");
			continue;
		}

		args->idx = i;
		args->socket_listener = socket_listener;

		socket_listener->event_loops[i] = ev_loop_new(0);
		if (socket_listener->event_loops[i] == NULL) {
			rdlog(LOG_ERR, "Can't create even't loop %zu", i);
			free(args);
			continue;
		}

		ev_set_userdata(socket_listener->event_loops[i], args);

		rd_fifoq_init(&socket_listener->watchers_queue[i]);
		ev_async_init(&socket_listener->event_asyncs[i], async_cb);
		socket_listener->event_asyncs[i].data = socket_listener;
		ev_async_start(socket_listener->event_loops[i],
			       &socket_listener->event_asyncs[i]);

		pthread_create(&socket_listener->threads[i],
			       NULL,
			       worker,
			       args);
	}

	ev_run(socket_listener->event_loop, 0);

	for (i = 0; i < socket_listener->config.threads; ++i) {
		if (NULL == socket_listener->event_loops[i]) {
			rdlog(LOG_ERR,
			      "Something happened: event loop %zu not found.",
			      i);
			continue;
		}

		ev_async_send(socket_listener->event_loops[i],
			      &socket_listener->event_asyncs[i]);
		pthread_join(socket_listener->threads[i], NULL);

		ev_async_stop(socket_listener->event_loops[i],
			      &socket_listener->event_asyncs[i]);
		ev_loop_destroy(socket_listener->event_loops[i]);
	}

	ev_async_stop(socket_listener->event_loop, &socket_listener->w_async);
	ev_io_stop(socket_listener->event_loop, &w_accept);

	ev_loop_destroy(socket_listener->event_loop);
}

/// @TODO join with TCP
static void *main_consumer_loop_udp(void *_thread_info) {
	struct udp_thread_info *thread_info = _thread_info;
	while (!do_shutdown) {
		struct sockaddr_in6 addr;
		char addr_buf[BUFSIZ];
		int recv_result = 0;
		struct timeval tv = {.tv_sec = 1, .tv_usec = 0};
		char *buffer = calloc(READ_BUFFER_SIZE, sizeof(char));
		pthread_mutex_lock(&thread_info->listenfd_mutex);
		if (likely(!do_shutdown)) {
			int select_result = select_socket(thread_info->listenfd,
							  &tv);
			if (select_result == -1 &&
			    errno != EINTR) { /* NOT
						 INTERRUPTED
					       */
				rdlog(LOG_ERR,
				      "listen select error: %s",
				      gnu_strerror_r(errno));
			} else if (select_result > 0) {
				recv_result = receive_from_socket(
						thread_info->listenfd,
						&addr,
						buffer,
						READ_BUFFER_SIZE);
			}
		}
		pthread_mutex_unlock(&thread_info->listenfd_mutex);

		if (recv_result < 0) {
			if (errno == EAGAIN) {
				rdbg("Socket not ready. re-trying");
				free(buffer);
			} else {
				rdlog(LOG_ERR,
				      "Recv error: %s",
				      gnu_strerror_r(errno));
				free(buffer);
				break;
			}
		} else {
			const char *client_addr =
					sockaddr2str(addr_buf,
						     sizeof(addr_buf),
						     (struct sockaddr *)&addr);
			process_data_received_from_socket(
					buffer,
					(size_t)recv_result,
					client_addr,
					thread_info->listener);
		}
	}

	return NULL;
}

static void
main_udp_loop(int listenfd, size_t udp_threads, const struct listener *l) {
	/* Lots of threads listening  and processing*/
	unsigned int i;
	struct udp_thread_info udp_thread_info;
	udp_thread_info.listenfd = listenfd;
	udp_thread_info.listener = l;

	assert(udp_threads > 0);
	pthread_t *threads = malloc(sizeof(threads[0]) * udp_threads);

	if (0 != createListenSocketMutex(&udp_thread_info.listenfd_mutex))
		exit(-1);

	for (i = 0; i < udp_threads; ++i)
		pthread_create(&threads[i],
			       NULL,
			       main_consumer_loop_udp,
			       &udp_thread_info);

	for (i = 0; i < udp_threads; ++i)
		pthread_join(threads[i], NULL);

	pthread_mutex_destroy(&udp_thread_info.listenfd_mutex);

	free(threads);
}

static void *main_socket_loop(void *vsocket_listener) {
	struct socket_listener *socket_listener = vsocket_listener;

	if (NULL == vsocket_listener) {
		rdlog(LOG_ERR, "NULL passed to %s", __FUNCTION__);
		return NULL;
	}

	int listenfd = createListenSocket(socket_listener->config.proto,
					  socket_listener->listener.port);
	if (listenfd == -1)
		return NULL;

	/*
	@TODO have to look at ev_set_syserr_cb
	*/

	if (0 == strcmp(N2KAFKA_UDP, socket_listener->config.proto)) {
		main_udp_loop(listenfd,
			      socket_listener->config.threads,
			      &socket_listener->listener);
	} else {
		main_tcp_loop(listenfd, socket_listener);
	}

	rdlog(LOG_INFO, "Closing listening socket.");
	close(listenfd);

	return NULL;
}

static void join_listener_socket(struct listener *slistener) {
	struct socket_listener *socket_listener =
			(struct socket_listener *)slistener;

	do_shutdown = 1;
	ev_async_send(socket_listener->event_loop, &socket_listener->w_async);
	pthread_join(socket_listener->main_loop, NULL);
	listener_join(&socket_listener->listener);
	free(socket_listener);
}

static struct listener *
create_socket_listener(const struct json_t *const_config,
		       const struct n2k_decoder *decoder) {
	json_t *config = json_deep_copy(const_config);
	if (NULL == config) {
		rdlog(LOG_ERR, "Couldn't clone JSON (OOM?)");
		return NULL;
	}
	json_error_t error;
	char *proto;
	json_int_t port;

	struct socket_listener *socket_listener =
			calloc(1, sizeof(*socket_listener));
	if (NULL == socket_listener) {
		rdlog(LOG_ERR, "Can't allocate private data (out of memory?)");
		goto calloc_err;
	}

	/* Defaults */
	socket_listener->config.threads = 1;
	socket_listener->config.tcp_keepalive = 0;
	socket_listener->config.thread_mode = MODE_EPOLL;
	const char *mode = NULL;

	const int unpack_rc =
			json_unpack_ex(config,
				       &error,
				       0,
				       "{s:s,s:i,s?i,s?b,s?s}",
				       "proto",
				       &proto,
				       "port",
				       &port,
				       "num_threads",
				       &socket_listener->config.threads,
				       "tcp_keepalive",
				       &socket_listener->config.tcp_keepalive,
				       "mode",
				       &mode);

	if (unpack_rc != 0 /* Failure */) {
		rdlog(LOG_ERR, "Can't decode listener: %s", error.text);
		free(socket_listener);
		return NULL;
	}

	if (socket_listener->config.threads == 0) {
		rdlog(LOG_ERR, "UDP threads has to be > 0. Setting to 1");
		socket_listener->config.threads = 1;
	}

	if (socket_listener->config.threads > MAX_NUM_THREADS) {
		rdlog(LOG_ERR,
		      "UDP threads has to be < %d. Setting to %d",
		      MAX_NUM_THREADS,
		      MAX_NUM_THREADS);
		socket_listener->config.threads = MAX_NUM_THREADS;
	}

	if (mode != NULL) {
		socket_listener->config.thread_mode = thread_mode_str(mode);
	}

	socket_listener->config.proto = strdup(proto);
	if (NULL == socket_listener->config.proto) {
		rdlog(LOG_ERR, "Error: Can't strdup protocol (out of memory?)");
		free(socket_listener);
		return NULL;
	}

	rdlog(LOG_INFO,
	      "Creating new %s listener on port %d",
	      proto,
	      (int)port);

	const int listener_init_rc = listener_init(
			&socket_listener->listener, port, decoder, config);

	if (listener_init_rc) {
		goto listener_init_err;
	}

	const int pcreate_rc = pthread_create(&socket_listener->main_loop,
					      NULL,
					      main_socket_loop,
					      socket_listener);
	if (pcreate_rc != 0) {
		rdlog(LOG_ERR,
		      "Couldn't create pthread: %s",
		      gnu_strerror_r(errno));
		goto pthread_create_err;
	}

	socket_listener->listener.join = join_listener_socket;
	return &socket_listener->listener;

pthread_create_err:
	socket_listener->listener.join(&socket_listener->listener);

listener_init_err:
	free(socket_listener);

calloc_err:
	json_decref(config);
	return NULL;
}

static const char *tcp_listener_name() {
	return "tcp";
}

static const char *udp_listener_name() {
	return "ucp";
}

// clang-format off
const n2k_listener_factory tcp_listener_factory = {
	.name = tcp_listener_name,
	.create = create_socket_listener,
}, udp_listener_factory = {
	.name = udp_listener_name,
	.create = create_socket_listener,
};
