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
#include "../include/BlockStore.h"

namespace
{
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
}

OOKv::BlockStore::BlockStore() :
		m_cache(512),
		m_write_inprogress(false)
{
}

OOKv::BlockStore::~BlockStore()
{
}

int OOKv::BlockStore::open(const char* path, bool read_only)
{
	// Find the store file

	// Check for checkpoint file
	if (1 /* Have checkpoint file*/)
	{
		// The blockstore crashed during a checkpoint, attempt recovery

		// We need to have read-write access
		if (read_only)
		{
			// Close store file and re-open read/write
		}

		// Playback checkpoint file, updating store file

		// Sync store file

		// Delete checkpoint file

		if (read_only)
		{
			// Close store file and re-open read-only
		}
	}

	// Read the header block and init

	// Find the journal file

	if (!read_only)
	{
		if (1 /*Have journal file*/)
		{
			int err = do_checkpoint();
			if (err != 0)
				return err;
		}
		else
		{
			// Create a journal file
		}
	}

	// Success!
	m_bReadOnly = read_only;
	return 0;
}

int OOKv::BlockStore::close()
{
	if (!m_bReadOnly)
	{
		int err = checkpoint();
		if (err != 0)
			return err;
	}


	// More?

	return 0;
}

OOKv::id_t OOKv::BlockStore::begin_read_transaction(int& err)
{
	OOBase::Guard<OOBase::RWMutex> guard(m_lock);

	err = m_read_transactions.insert(m_latest_transaction);
	if (err != 0)
		return 0;

	return m_latest_transaction;
}

int OOKv::BlockStore::end_read_transaction(const id_t& trans_id)
{
	OOBase::Guard<OOBase::RWMutex> guard(m_lock);

	return m_read_transactions.remove(trans_id);
}

OOKv::id_t OOKv::BlockStore::begin_write_transaction(int& err, const OOBase::Countdown& countdown)
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

int OOKv::BlockStore::commit_write_transaction(const id_t& trans_id)
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
	if (trans_id % m_checkpoint_interval == 0)
	{
		// Ignore error... it will get picked up again...
		do_checkpoint();
	}

	m_write_inprogress = false;
	m_write_condition.signal();

	return 0;
}

void OOKv::BlockStore::rollback_write_transaction(const id_t& trans_id)
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

int OOKv::BlockStore::checkpoint(const OOBase::Countdown& countdown)
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

OOKv::BlockStore::Block OOKv::BlockStore::get_block(const id_t& block_id, const id_t& trans_id, int& err)
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

int OOKv::BlockStore::update_block(const id_t& block_id, const id_t& trans_id, Block block)
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

int OOKv::BlockStore::free_block(const id_t& block_id, const id_t& trans_id)
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

OOKv::id_t OOKv::BlockStore::alloc_block(const id_t& trans_id, Block& block, int& err)
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

int OOKv::BlockStore::do_checkpoint()
{
	OOBase::ReadGuard<OOBase::RWMutex> read_guard(m_lock);

	id_t earliest_transaction = m_latest_transaction;
	if (!m_read_transactions.empty())
		earliest_transaction = *m_read_transactions.at(0);

	read_guard.release();

	// Play forward journal to earliest_transaction

	// Update each block, writing new to checkpoint file

	// Sync checkpoint file

	// Truncate journal if possible

	// Play forward checkpoint file, writing each block to store file

	// Sync store file

	// Delete checkpoint file

	return 0;
}
