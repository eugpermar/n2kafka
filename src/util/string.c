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

#include "string.h"

#include "util/util.h"

#include <librd/rdlog.h>

#include <assert.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <syslog.h>

#ifndef static_assert
#define static_assert(...)
#endif

#define STRING_MIN_SIZE 4096

#define smax(a, b)                                                             \
	({                                                                     \
		typeof(a) __a = a, __b = b;                                    \
		(__a > __b) ? __a : __b;                                       \
	})

static size_t next_pow2(size_t x) {
	static_assert(sizeof(x) == sizeof(unsigned long),
		      "Please expand this function!");
	// bits of size_t
	static const size_t bits_x = CHAR_BIT * sizeof(x);
	return (x && x - 1) ? 1u << (bits_x - (size_t)__builtin_clzl(x - 1))
			    : 1;
}

static size_t string_allocated(const string *str) {
	return !str->buf ? 0
			 : str->size < STRING_MIN_SIZE ? STRING_MIN_SIZE
						       : next_pow2(str->size);
}

static size_t string_grow(string *str, size_t delta) {
	const size_t new_size = str->size + delta;
	const size_t new_allocated_size = next_pow2(new_size);
	if (!(string_allocated(str) < new_allocated_size)) {
		// No need to grow
		return string_allocated(str);
	}

	const size_t new_allocated = smax(new_allocated_size, STRING_MIN_SIZE);
	char *new_buf = realloc(str->buf, new_allocated);
	if (NULL == new_buf) {
		// Failure reallocating
		return string_allocated(str);
	}

	str->buf = new_buf;
	return new_allocated;
}

int string_append(string *str, const char *data, size_t data_size) {
	assert(data_size == 0 || NULL != data);
	const size_t new_size = string_grow(str, data_size);
	if (unlikely(new_size < str->size + data_size)) {
		// Can't append
		return -1;
	}

	memcpy(&str->buf[str->size], data, data_size);
	str->size += data_size;
	return 0;
}

/// Append message to string using printf format.
int string_printf(string *str, const char *fmt, ...) {
	va_list args_bak;

	assert(str);
	assert(fmt);

	va_start(args_bak, fmt);
	const size_t initial_len = string_size(str);
	bool done = false;
	int rc = 0;
	while (!done) {
		va_list args;
		va_copy(args, args_bak);

		const size_t available =
				string_allocated(str) - string_size(str);

		const size_t printf_rc =
				(size_t)vsnprintf(str->buf + string_size(str),
						  available,
						  fmt,
						  args);
		if (likely(available > printf_rc + sizeof((char)'\0'))) {
			str->size += printf_rc;
			rc = printf_rc;
			done = true;
		} else {
			const size_t new_len = string_grow(
					str, initial_len + printf_rc);
			if (unlikely(new_len == initial_len)) {
				rdlog(LOG_ERR,
				      "Couldn't print response string (OOM?)");
				str->buf[initial_len] = '\0';
				rc = -1;
				done = true;
			}
		}

		va_end(args);
	}

	va_end(args_bak);
	return rc;
}

int string_append_string(string *str, const char *data) {
	return string_append(str, data, strlen(data));
}

void string_pop_back(string *str) {
	assert(str->buf);
	assert(str->size > 0);
	str->buf[--str->size] = '\0';
}

static int print_not_utf8_code(string *str, char c) {
	return string_printf(str, "\\\\x%u", *(uint8_t *)&c);
}

static size_t multibyte_utf8_unicode_size(const char *data, size_t data_size) {
	assert(data_size > 0);
	const size_t leading_ones =
			(size_t)__builtin_clz(~((unsigned)data[0] << 24));

	switch (leading_ones) {
	case 0:
		// ASCII case
		return 1;
	case 2:
	case 3:
	case 4:
		break;
	default:
		// Surely wrong
		return 0;
	};

	if (data_size < leading_ones) {
		// Not UTF-8
		return 0;
	}

	for (size_t i = 1; i < leading_ones; ++i) {
		// All UTF-8 trailing bytes must start with 10xxxxxx
		if ((data[i] & 0xc0) != 0x80) {
			return 0;
		}
	}

	return leading_ones;
}

/**
 * @brief      Print a (supposed) UTF-8 character. If not possible, print a hint
 *
 * @param      str        The string to print
 * @param[in]  data       The byte array containing characters
 * @param[in]  data_size  The data size
 *
 * @return     Number of charactered consumed from data. 0 in case of error.
 */
static int print_utf8(string *str, const char *data, size_t data_size) {
	assert(data_size > 0);

	const size_t character_size =
			multibyte_utf8_unicode_size(data, data_size);
	if (0 == character_size) {
		// Just print an user guide
		const int append_rc = print_not_utf8_code(str, data[0]);
		return append_rc > 0 ? 1 : 0;
	} else {
		const int append_rc = string_append(str, data, character_size);
		return append_rc == 0 ? (int)character_size : -1;
	}
}

static int print_json_escaped_character(string *str, char c) {
	switch (c) {
	case '\b':
		return string_append_string(str, "\\b");
	case '\f':
		return string_append_string(str, "\\f");
	case '\n':
		return string_append_string(str, "\\n");
	case '\r':
		return string_append_string(str, "\\r");
	case '\t':
		return string_append_string(str, "\\t");
	default:
		return print_not_utf8_code(str, c) > 0;
	};
}

int string_append_json_string(string *str, const char *data, size_t data_size) {
	size_t i = 0;
	int rc = 0;
	while (rc == 0 && i < data_size) {
		if (strchr("\b\f\n\r\t", data[i])) {
			if (0 != print_json_escaped_character(str, data[i])) {
				rc = 1;
			}
			++i;
			continue;
		}

		static const char auto_escape[] = "\"\\";
		if (strchr(auto_escape, data[i])) {
			if (0 != string_append_string(str, "\\")) {
				rc = 1;
				break;
			}

			if (0 != string_append(str, &data[i], 1)) {
				rc = 1;
				break;
			}
			++i;
			continue;
		}

		const int consumed_utf8 =
				print_utf8(str, &data[i], data_size - i);
		if (consumed_utf8 < 0) {
			rc = 1;
			break;
		}

		i += (size_t)consumed_utf8;
	}

	return rc;
}
