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

#ifndef OOKV_BLOCKSTORE_H_INCLUDED_
#define OOKV_BLOCKSTORE_H_INCLUDED_

#if defined(HAVE_STDINT_H)
#include <stdint.h>
#endif

#include <OOBase/SmartPtr.h>

namespace OOKv
{

#if defined(_MSC_VER)
	typedef unsigned __int64 id_t;
#elif defined(HAVE_STDINT_H)
#include <stdint.h>
	typedef ::uint64_t id_t;
#else
#error Failed to work out a base type for unsigned 64bit integer.
#endif

	class BlockStore : public OOBase::RefCounted
	{
	public:
		static const size_t s_block_size = 4096;

		static BlockStore* open(const char* path, bool read_only, int& err);

		virtual id_t begin_read_transaction(int& err) = 0;
		virtual int end_read_transaction(const id_t& trans_id) = 0;

		virtual id_t begin_write_transaction(int& err, const OOBase::Countdown& countdown = OOBase::Countdown()) = 0;
		virtual int commit_write_transaction(const id_t& trans_id) = 0;
		virtual void rollback_write_transaction(const id_t& trans_id) = 0;

		virtual int checkpoint(const OOBase::Countdown& countdown = OOBase::Countdown()) = 0;

		typedef OOBase::SmartPtr<void*> Block;

		virtual Block get_block(const id_t& block_id, const id_t& trans_id, int& err) = 0;

		virtual int update_block(const id_t& block_id, const id_t& trans_id, Block block) = 0;
		virtual id_t alloc_block(const id_t& trans_id, Block& block, int& err) = 0;
		virtual int free_block(const id_t& block_id, const id_t& trans_id) = 0;

	protected:
		BlockStore() : OOBase::RefCounted() {};
	};
}

#endif // OOKV_BLOCKSTORE_H_INCLUDED_
