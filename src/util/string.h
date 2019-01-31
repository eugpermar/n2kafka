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

#pragma once

#include <stdlib.h>
#include <string.h>

/// String with automatic grow
typedef struct string {
	char *buf;   ///< Actual buffer
	size_t size; ///< Size occupied in buffer. Allocated size will always
		     ///< be (log2(size)<<1), with a minimum of STRING_MIN_SIZE.
} string;

#define N2K_STRING_INITIALIZER                                                 \
	(string) {                                                             \
	}

static inline int string_init(struct string *str) {
	*str = N2K_STRING_INITIALIZER;
	return 0;
}

static inline void string_done(struct string *str) {
	free(str->buf);
	memset(str, 0, sizeof(*str));
}

static inline size_t string_size(const string *str) {
	return str->size;
}

int string_append(string *str, const char *data, size_t data_size);

/** Append a zero-terminated string buffer to string

 @param      str    The string
 @param[in]  str_b  The append string

 @return     0 if OK, NULL otherwise
*/
int string_append_string(string *str, const char *str_b);

/** Append a valid JSON as string. It does append first and last quotes.

 @param      str        The string
 @param[in]  data       The string to append
 @param[in]  data_size  The size of data

 @return     0 if OK, NULL otherwise
*/
int string_append_json_string(string *str, const char *data, size_t data_size);

/**
 @brief      Erases the last character of the string, effectively reducing its
	     length by one.

 @param      str   The string
*/
void string_pop_back(string *str);

/** Append message to string using printf format.

 @param      str        The string
 @param[in]  fmt        The format
 @param[in]  <unnamed>  Parameters as printf accept

 @return     Same as sprintf
*/
int string_printf(string *str, const char *fmt, ...)
		__attribute__((format(printf, 2, 3)));
