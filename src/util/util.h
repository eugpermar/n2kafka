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

#include <librd/rdlog.h>

#include <string.h>

#ifdef likely
#undef likely
#endif
#define likely(x) __builtin_expect(!!(x), 1)

#ifdef unlikely
#undef unlikely
#endif
#define unlikely(x) __builtin_expect(!!(x), 0)

#define rblog(x...) rdlog(x)

#define fatal(msg...)                                                          \
	do {                                                                   \
		rblog(LOG_ERR, msg);                                           \
		exit(1);                                                       \
	} while (0)

#define swap_ptrs(p1, p2)                                                      \
	do {                                                                   \
		void *aux = p1;                                                \
		p1 = p2;                                                       \
		p2 = aux;                                                      \
	} while (0)

/// Remove constness. Use with caution.
static inline void *const_cast(const void *ptr) {
	void *ret;
	memcpy(&ret, &ptr, sizeof(ret));
	return ret;
}

/**
 * @brief      Adapts the strerror function to the one available in the system
 *
 * @param[in]  t_errno  The errno
 *
 * @return     String describing the error.
 */
const char *gnu_strerror_r(int t_errno);
