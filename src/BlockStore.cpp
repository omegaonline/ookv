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

#include "../include/config-kv.h"

#include <OOBase/Cache.h>
#include <OOBase/Set.h>
#include <OOBase/Condition.h>
#include <OOBase/CDRStream.h>

#include "../include/BlockStore.h"
#include "File.h"

using namespace OOKv;

namespace
{
	const size_t s_checkpoint_interval = 256;

	namespace LogRecord
	{
		enum Type
		{
			Alloc = 0,
			Free,
			Diff,
			Commit,

			MAX
		};
	}

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

		bool operator < (const id_t& id) const
		{
			return (m_block_id < id);
		}

		bool operator == (const BlockSpan& rhs) const
		{
			return (m_block_id == rhs.m_block_id && m_start_trans_id == rhs.m_start_trans_id);
		}

		bool operator < (const BlockSpan& rhs) const
		{
			return (m_block_id < rhs.m_block_id || (m_block_id == rhs.m_block_id && m_start_trans_id < rhs.m_start_trans_id));
		}
	};

	class BlockStoreBase : public OOKv::BlockStore
	{
	public:
		BlockStoreBase();

		virtual int open_i(const char* path) = 0;

		int load_store();

		id_t begin_read_transaction(int& err);
		int end_read_transaction(const id_t& trans_id);

		Block get_block(const id_t& block_id, const id_t& trans_id, int& err);

		// Persistent data
		id_t m_latest_transaction;
		id_t m_free_list_head_block;

		// Volatile data - controlled by m_lock
		OOBase::RWMutex                     m_lock;
		OOBase::Set<id_t>                   m_read_transactions;
		OOBase::TableCache<BlockSpan,Block> m_cache;
		File                                m_store_file;

	private:
		Block load_block(const id_t& block_id, id_t& start_trans_id, int& err);
		int apply_journal(Block& block, const BlockSpan& from, const id_t& to);
	};

	class BlockStoreRO : public BlockStoreBase
	{
	public:
		int open_i(const char* path);

		id_t begin_write_transaction(int& err, const OOBase::Countdown& countdown = OOBase::Countdown()) { err=EROFS; return 0;}
		int commit_write_transaction(const id_t& trans_id) { return EROFS; }
		void rollback_write_transaction(const id_t& trans_id) {}

		int checkpoint(const OOBase::Countdown& countdown = OOBase::Countdown()) { return EROFS; }

		int update_block(const id_t& block_id, const id_t& trans_id, Block block) { return EROFS; }
		id_t alloc_block(const id_t& trans_id, Block& block, int& err) { err=EROFS; return 0; }
		int free_block(const id_t& block_id, const id_t& trans_id) { return EROFS; }
	};

	class BlockStoreRW : public BlockStoreBase
	{
	public:
		BlockStoreRW();
		~BlockStoreRW();

		int open_i(const char* path);

		id_t begin_write_transaction(int& err, const OOBase::Countdown& countdown = OOBase::Countdown());
		int commit_write_transaction(const id_t& trans_id);
		void rollback_write_transaction(const id_t& trans_id);

		int checkpoint(const OOBase::Countdown& countdown = OOBase::Countdown());

		int update_block(const id_t& block_id, const id_t& trans_id, Block block);
		id_t alloc_block(const id_t& trans_id, Block& block, int& err);
		int free_block(const id_t& block_id, const id_t& trans_id);

	private:
		// Volatile data - controlled by m_write_lock
		OOBase::Condition::Mutex       m_write_lock;
		OOBase::Condition              m_write_condition;
		bool                           m_write_inprogress;
		OOBase::CDRStream              m_log;

		int do_checkpoint();
		int apply_checkpoint(File& checkpoint, const OOBase::LocalString& checkpoint_path);
	};

	template <typename T>
	OOBase::RefPtr<OOKv::BlockStore> open_t(const char* path, int& err)
	{
		OOBase::RefPtr<T> store = new (std::nothrow) T();
		if (!store)
			err = ERROR_OUTOFMEMORY;
		else if ((store->open_i(path)) != 0)
		{
			store->release();
			store = NULL;
		}

		return static_cast<BlockStore*>(store);
	}
}

OOBase::RefPtr<OOKv::BlockStore> OOKv::BlockStore::open(const char* path, bool read_only, int& err)
{
	if (read_only)
		return open_t<BlockStoreRO>(path,err);
	else
		return open_t<BlockStoreRW>(path,err);
}

BlockStoreBase::BlockStoreBase() :
		m_cache(512)
{
}

OOKv::id_t BlockStoreBase::begin_read_transaction(int& err)
{
	OOBase::Guard<OOBase::RWMutex> guard(m_lock);

	err = m_read_transactions.insert(m_latest_transaction);
	if (err != 0)
		return 0;

	return m_latest_transaction;
}

int BlockStoreBase::end_read_transaction(const id_t& trans_id)
{
	OOBase::Guard<OOBase::RWMutex> guard(m_lock);

	return m_read_transactions.remove(trans_id);
}

BlockStore::Block BlockStoreBase::get_block(const id_t& block_id, const id_t& trans_id, int& err)
{
	if (trans_id > m_latest_transaction)
	{
		err = EINVAL;
		return Block();
	}

	OOBase::ReadGuard<OOBase::RWMutex> read_guard(m_lock);

	BlockSpan span(block_id,0);
	Block block;

	// This is a prefix lookup, that will land somewhere in the set of transactions in the cache
	size_t pos = m_cache.find_at(block_id);
	if (pos != m_cache.npos)
	{
		// If we find an entry then we need to shuffle forwards and back until we hit the nearest
		for (;pos < m_cache.size()-1; ++pos)
		{
			const BlockSpan* b = m_cache.key_at(pos+1);
			if (b->m_block_id != block_id || b->m_start_trans_id >= trans_id)
				break;
		}

		for (;pos > 0; --pos)
		{
			const BlockSpan* b = m_cache.key_at(pos-1);
			if (b->m_block_id != block_id || b->m_start_trans_id <= trans_id)
				break;
		}

		span = *m_cache.key_at(pos);

		if (span.m_start_trans_id <= trans_id)
		{
			block = *m_cache.at(pos);
			if (span.m_start_trans_id == trans_id)
				return block;
		}
		else
			span.m_start_trans_id = 0;
	}

	read_guard.release();

	if (!block)
	{
		// Load up the first block in the file
		block = load_block(block_id,span.m_start_trans_id,err);
		if (err != 0)
			return Block();
	}

	// Play forward journal till trans_id
	if (span.m_start_trans_id < trans_id)
	{
		err = apply_journal(block,span,trans_id);
		if (err != 0)
			return Block();

		span.m_start_trans_id = trans_id;
	}

	OOBase::Guard<OOBase::RWMutex> write_guard(m_lock);

	// Add the block to the cache
	m_cache.insert(span,block);
	return block;
}

int BlockStoreRO::open_i(const char* path)
{
	// Find the store file
	if (!File::exists(path))
		return ENOENT;

	// Open read only
	int err = m_store_file.open(path,true);
	if (err == 0)
		err = load_store();

	return err;
}

BlockStoreRW::BlockStoreRW() : BlockStoreBase(),
		m_write_inprogress(false)
{
}

BlockStoreRW::~BlockStoreRW()
{
	checkpoint();
}

int BlockStoreRW::open_i(const char* path)
{
	// Find the store file
	if (!File::exists(path))
		return ENOENT;

	// Build the filenames
	OOBase::LocalString checkpoint_path, journal_path;
	int err = checkpoint_path.concat(path,".checkpoint");
	if (err == 0)
		err = journal_path.concat(path,".journal");
	if (err != 0)
		return err;

	// Open read/write
	if ((err = m_store_file.open(path,false)) != 0)
		return err;

	// Load the store
	if ((err = load_store()) != 0)
		return err;

	// Check for checkpoint file
	if (File::exists(checkpoint_path.c_str()))
	{
		// The blockstore crashed during a checkpoint, attempt recovery

		// Open checkpoint file read only
		File checkpoint;
		if ((err = checkpoint.open(checkpoint_path.c_str(),true)) == 0)
		{
			err = apply_checkpoint(checkpoint,checkpoint_path);
		}

		if (err != 0)
			return err;
	}

	// Find the journal file
	if (File::exists(journal_path.c_str()))
	{
		// Ignore errors, the store is safe anyway
		do_checkpoint();
	}

	return 0;
}

OOKv::id_t BlockStoreRW::begin_write_transaction(int& err, const OOBase::Countdown& countdown)
{
	// Acquire the lock with a timeout
	OOBase::Guard<OOBase::Condition::Mutex> guard(m_write_lock,false);
	if (!guard.acquire(countdown))
	{
		err = ETIMEDOUT;
		return 0;
	}

	while (m_write_inprogress)
	{
		if (!m_write_condition.wait(m_write_lock,countdown))
		{
			err = ETIMEDOUT;
			return 0;
		}
	}

	err = m_log.reset();
	if (err != 0)
		return 0;

	m_write_inprogress = true;

	return m_latest_transaction+1;
}

int BlockStoreRW::commit_write_transaction(const id_t& trans_id)
{
	OOBase::Guard<OOBase::Condition::Mutex> guard(m_write_lock);

	if (!m_write_inprogress || trans_id != m_latest_transaction+1)
		return EACCES;

	// Write a commit record to the log
	if (!m_log.write(static_cast<char>(LogRecord::Commit)) ||
			!m_log.write(trans_id))
	{
		return m_log.last_error();
	}

	// Write the log to the journal

	// Sync the journal

	m_log.reset();
	m_latest_transaction = trans_id;

	// Check for checkpoint
	if (trans_id % s_checkpoint_interval == 0)
	{
		// Ignore error... it will get picked up again...
		do_checkpoint();
	}

	m_write_inprogress = false;
	m_write_condition.signal();

	return 0;
}

void BlockStoreRW::rollback_write_transaction(const id_t& trans_id)
{
	OOBase::Guard<OOBase::Condition::Mutex> guard(m_write_lock);

	// Discard log contents
	m_log.reset();

	if (m_write_inprogress && trans_id == m_latest_transaction+1)
	{
		m_write_inprogress = false;
		m_write_condition.signal();
	}
}

int BlockStoreRW::checkpoint(const OOBase::Countdown& countdown)
{
	// Acquire the lock with a timeout
	OOBase::Guard<OOBase::Condition::Mutex> guard(m_write_lock,false);
	if (!guard.acquire(countdown))
		return ETIMEDOUT;

	while (m_write_inprogress)
	{
		if (!m_write_condition.wait(m_write_lock,countdown))
			return ETIMEDOUT;
	}

	m_write_inprogress = true;

	int err = do_checkpoint();

	m_write_inprogress = false;
	m_write_condition.signal();

	return err;
}

int BlockStoreRW::update_block(const id_t& block_id, const id_t& trans_id, Block block)
{
	// This is not a 100% race-safe check, but it will help!
	if (!m_write_inprogress || trans_id != m_latest_transaction+1)
		return EACCES;

	// Get the previous value...
	if (trans_id <= 1)
		return EINVAL;

	int err = 0;
	Block prev_block = get_block(block_id,trans_id-1,err);
	if (err != 0)
		return err;

	// Write a diff block to the log
	if (!m_log.write(static_cast<char>(LogRecord::Diff)) ||
			!m_log.write(trans_id) ||
			!m_log.write(block_id))
	{
		return m_log.last_error();
	}

	// Write the diff of old_block -> block to the log

	if (err == 0)
	{
		// Update cache
		OOBase::Guard<OOBase::RWMutex> guard(m_lock);
		m_cache.insert(BlockSpan(block_id,trans_id),block);
	}

	return err;
}

int BlockStoreRW::free_block(const id_t& block_id, const id_t& trans_id)
{
	// This is not a 100% race-safe check, but it will help!
	if (!m_write_inprogress || trans_id != m_latest_transaction+1)
		return EACCES;

	// Get the previous value...
	if (trans_id <= 1)
		return EINVAL;

	// Write a free block record to the log
	if (!m_log.write(static_cast<char>(LogRecord::Free)) ||
			!m_log.write(trans_id) ||
			!m_log.write(block_id))
	{
		return m_log.last_error();
	}

	return 0;
}

OOKv::id_t BlockStoreRW::alloc_block(const id_t& trans_id, Block& block, int& err)
{
	// This is not a 100% race-safe check, but it will help!
	if (!m_write_inprogress || trans_id != m_latest_transaction+1)
	{
		err = EACCES;
		return 0;
	}

	// Allocate new block from somewhere
	void* TODO;

	id_t block_id = 1;

	// Write an alloc record to the log
	if (!m_log.write(static_cast<char>(LogRecord::Alloc)) ||
			!m_log.write(trans_id) ||
			!m_log.write(block_id))
	{
		err = m_log.last_error();
		return 0;
	}

	if (err == 0)
	{
		// Update cache
		OOBase::Guard<OOBase::RWMutex> guard(m_lock);
		m_cache.insert(BlockSpan(block_id,trans_id),block);
	}

	return block_id;
}

int BlockStoreRW::do_checkpoint()
{
	OOBase::ReadGuard<OOBase::RWMutex> read_guard(m_lock);

	id_t earliest_transaction = m_latest_transaction;
	if (!m_read_transactions.empty())
		earliest_transaction = *m_read_transactions.at(0);

	read_guard.release();

	OOBase::LocalString checkpoint_path;
	int err = checkpoint_path.concat(path,".checkpoint");
	if (err != 0)
		return err;

	File checkpoint;
	if ((err = checkpoint.create(checkpoint_path.c_str(),false)) != 0)
		return err;

	// Play forward journal to earliest_transaction

	// Update each block, writing new to checkpoint file

	// Sync checkpoint file
	if ((err = checkpoint.sync()) != 0)
		return err;

	// Play forward checkpoint file, writing each block to store file
	if ((err = apply_checkpoint(checkpoint,checkpoint_path)) != 0)
		return err;

	// Truncate journal if possible

	return 0;
}

int BlockStoreRW::apply_checkpoint(File& checkpoint, const OOBase::LocalString& checkpoint_path)
{
	// Playback checkpoint file, updating store file
	int err = 0;



	// Sync store file
	if (err == 0 && (err = m_store_file.sync()) == 0)
	{
		checkpoint.close();

		// Delete checkpoint file
		File::remove(checkpoint_path.c_str());
	}

	return err;
}
