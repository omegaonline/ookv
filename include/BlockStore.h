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
#include <OOBase/CDRStream.h>

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

		int open(const char* path, bool read_only);
		int close();

		id_t begin_read_transaction(int& err);
		int end_read_transaction(const id_t& trans_id);

		id_t begin_write_transaction(int& err, const OOBase::Countdown& countdown = OOBase::Countdown());
		int commit_write_transaction(const id_t& trans_id);
		void rollback_write_transaction(const id_t& trans_id);

		int checkpoint(const OOBase::Countdown& countdown = OOBase::Countdown());

		typedef OOBase::SmartPtr<void*> Block;

		Block get_block(const id_t& block_id, const id_t& trans_id, int& err);

		int update_block(const id_t& block_id, const id_t& trans_id, Block block);
		id_t alloc_block(const id_t& trans_id, Block& block, int& err);
		int free_block(const id_t& block_id, const id_t& trans_id);

	private:
		BlockStore( const BlockStore& );
		BlockStore& operator = ( const BlockStore& );

		static const size_t m_checkpoint_interval = 256;

		struct BlockSpan
		{
			id_t m_block_id;
			id_t m_start_trans_id;

			BlockSpan(const id_t& block_id, const id_t& start_trans_id) :
					m_block_id(block_id), m_start_trans_id(start_trans_id)
			{}

			bool operator == (const id_t& id) const
			{
				return (m_block_id == id);
			}

			bool operator == (const BlockSpan& rhs) const
			{
				return (m_block_id == rhs.m_block_id && m_start_trans_id == rhs.m_start_trans_id);
			}

			bool operator < (const id_t& id) const
			{
				return (m_block_id < id);
			}

			bool operator < (const BlockSpan& rhs) const
			{
				return (m_block_id < rhs.m_block_id || (m_block_id == rhs.m_block_id && m_start_trans_id < rhs.m_start_trans_id));
			}
		};

		// Persistent data
		id_t m_latest_transaction;
		id_t m_free_list_head_block;

		// Uncontrolled data
		bool m_bReadOnly;

		// Volatile data - controlled by m_lock
		OOBase::RWMutex                     m_lock;
		OOBase::Set<id_t>                   m_read_transactions;
		OOBase::TableCache<BlockSpan,Block> m_cache;

		// Volatile data - controlled by m_write_lock
		OOBase::Condition::Mutex       m_write_lock;
		OOBase::Condition              m_write_condition;
		bool                           m_write_inprogress;
		Block                          m_block_zero;
		OOBase::CDRStream              m_log;

		int do_checkpoint();
		Block load_block(const id_t& block_id, id_t& start_trans_id, int& err);
		int apply_journal(Block& block, const BlockSpan& from, const id_t& to);
	};
}

#endif // OOKV_BLOCKSTORE_H_INCLUDED_
