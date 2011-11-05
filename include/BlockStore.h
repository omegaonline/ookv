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

#include <OOBase/Cache.h>
#include <OOBase/SmartPtr.h>
#include <OOBase/Set.h>
#include <OOBase/Condition.h>

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

	class BlockStore
	{
	public:
		BlockStore();
		~BlockStore();

		int open(const char* path, bool read_only, int& err);
		int close();

		id_t begin_read_transaction(int& err);
		int commit_read_transaction(const id_t& trans_id);

		id_t begin_write_transaction(int& err, const OOBase::Countdown& countdown = OOBase::Countdown());
		int commit_write_transaction(const id_t& trans_id);

		int checkpoint(const OOBase::Countdown& countdown);

		typedef OOBase::SmartPtr<void*> Block;

		Block get_block(const id_t& block_id, const id_t& trans_id, int& err);

		int update_block(const id_t& block_id, const id_t& trans_id, Block block);
		Block alloc_block(const id_t& trans_id, id_t& block_id, int& err);
		int free_block(const id_t& trans_id, const id_t& block_id);

	private:
		BlockStore( const BlockStore& );
		BlockStore& operator = ( const BlockStore& );

		static const size_t m_checkpoint_interval = 256;

		struct

		// Persistent data
		id_t m_current_transaction;
		id_t m_free_list_head_block;

		// Volatile data - controlled by m_lock
		OOBase::RWMutex                     m_lock;
		OOBase::Set<id_t>                   m_read_transactions;
		OOBase::TableCache<BlockSpan,Block> m_cache;

		// Volatile data - controlled by m_write_lock
		OOBase::Condition::Mutex m_write_lock;
		OOBase::Condition        m_write_condition;
		bool                     m_write_inprogress;
		Block                    m_block_zero;

		int do_checkpoint();
		int get_block_i(const id_t& block_id, const id_t& trans_id, BlockSpan& span, Block& block);
	};


}

#endif // OOKV_BLOCKSTORE_H_INCLUDED_
