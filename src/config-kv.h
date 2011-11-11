///////////////////////////////////////////////////////////////////////////////////
//
// Copyright (C) 2011 Rick Taylor
//
// This file is part of OOKv, the Omega Online Key/Value library.
//
// OOKv is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// OOKv is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with OOKv.  If not, see <http://www.gnu.org/licenses/>.
//
///////////////////////////////////////////////////////////////////////////////////

#ifndef OOKV_CONFIG_KV_H_INCLUDED_
#define OOKV_CONFIG_KV_H_INCLUDED_

#if defined(_MSC_VER)
	#include "ookv-msvc.h"
#elif !defined(DOXYGEN)
	// Autoconf
	#include <ookv-autoconf.h>
#endif

// Bring in oobase
#include <config-base.h>

#if defined(HAVE_STDINT_H)
#include <stdint.h>
#endif

namespace OOKv
{
#if defined(_MSC_VER)
	typedef __int64 int64_t;
	typedef unsigned __int64 uint64_t;

	typedef unsigned __int16 uint16_t;
#elif defined(HAVE_STDINT_H)
#include <stdint.h>
	using ::int64_t;
	using ::uint64_t;

	using ::uint16_t;
#else
#error Failed to work out a base type for unsigned 64bit integer.
#endif
}

#endif //OOKV_CONFIG_KV_H_INCLUDED_
