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

OOKv::BlockStore::BlockStore() :
		m_cache(512),
		m_write_inprogress(false)
{
}

OOKv::BlockStore::~BlockStore()
{
}

OOKv::id_t OOKv::BlockStore::begin_read_transaction(int& err)
{
	OOBase::Guard<OOBase::RWMutex> guard(m_lock);

	err = m_read_transactions.insert(m_current_transaction);
	if (err != 0)
		return 0;

	return m_current_transaction;
}

int OOKv::BlockStore::commit_read_transaction(const id_t& trans_id)
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

	m_write_inprogress = true;

	return m_current_transaction+1;
}

int OOKv::BlockStore::commit_write_transaction(const id_t& trans_id)
{
	OOBase::Guard<OOBase::Condition::Mutex> guard(m_write_lock);

	if (!m_write_inprogress || trans_id != m_current_transaction+1)
		return EACCES;

	int err = 0;

	// Write a commit record to the journal

	// Sync the journal

	// Check for checkpoint
	if (trans_id % m_checkpoint_interval == 0)
	{
		// Ignore error... it will get picked up again...
		do_checkpoint();
	}

	m_current_transaction = trans_id;
	m_write_inprogress = false;
	m_write_condition.signal();

	return err;
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
	while (span.m_start_trans_id < trans_id)
	{
		++span.m_start_trans_id;
		err = apply_journal(block,span);
		if (err != 0)
			return Block();
	}

	OOBase::Guard<OOBase::RWMutex> write_guard(m_lock);

	// Add the block to the cache
	m_cache.insert(span,block);
	return block;
}

int OOKv::BlockStore::update_block(const id_t& block_id, const id_t& trans_id, Block block)
{
	// This is not a 100% race-safe check, but it will help!
	if (!m_write_inprogress)
		return EACCES;

	// Get the previous value...
	if (trans_id <= 1)
		return EINVAL;

	int err = 0;
	Block prev_block = get_block(block_id,trans_id-1,err);
	if (err != 0)
		return err;

	// Write the diff of old_block -> block to the journal

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
	if (!m_write_inprogress)
		return EACCES;

	// Get the previous value...
	if (trans_id <= 1)
		return EINVAL;

	// Write a free record to the journal

	return 0;
}

OOKv::id_t OOKv::BlockStore::alloc_block(const id_t& trans_id, Block& block, int& err)
{
	// Allocate new block from somewhere
	id_t block_id = 1;

	// Write an alloc record to the journal

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
	// Replay journal, updating the store file!

	// m_current_transaction = last commit

	return 0;
}
