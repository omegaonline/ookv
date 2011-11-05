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
	BlockSpan span;
	Block block;

	err = get_block_i(block_id,trans_id,span,block);

	return block;
}

int OOKv::BlockStore::get_block_i(const id_t& block_id, const id_t& trans_id, BlockSpan& span, Block& block)
{
	OOBase::ReadGuard<OOBase::RWMutex> read_guard(m_lock);

	BlockRef ref = { block_id, trans_id };

	size_t pos = m_cache.find_first(ref);
	if (pos != m_cache.npos)
	{
		span = *m_cache.key_at(pos);
		block = *m_cache.at(pos);
		if (block)
			return 0;
	}

	read_guard.release();

	span.m_block_id = block_id;
	span.m_trans_start_id = 0;
	span.m_trans_end_id = 0;

	// Load it...
	int err = 0;


	// Play forward journal till trans_id

	return ENOENT;
}

int OOKv::BlockStore::update_block(const id_t& block_id, const id_t& trans_id, Block block)
{
	// This is not a 100% race-safe check, but it will help!
	if (!m_write_inprogress)
		return EACCES;

	Block old_block;
	BlockSpan span;
	int err = get_block_i(block_id,trans_id-1,span,old_block);
	if (err != 0)
		return err;

	// Write the diff of old_block -> block to the journal

	if (err == 0)
	{
		// Update cache
		OOBase::Guard<OOBase::RWMutex> guard(m_lock);

		m_cache.replace(span,NULL);
		span.m_trans_end_id = trans_id-1;
		m_cache.insert(span,old_block);

		span.m_trans_start_id = trans_id;
		span.m_trans_end_id = 0;
		m_cache.insert(span,block);
	}

	return err;
}

int OOKv::BlockStore::free_block(const id_t& trans_id, const id_t& block_id)
{
	// This is not a 100% race-safe check, but it will help!
	if (!m_write_inprogress)
		return EACCES;

	// Get the previous value...
	Block old_block;
	BlockSpan span;
	int err = get_block_i(block_id,trans_id-1,span,old_block);
	if (err != 0)
		return err;

	// Write a free record to the journal

	if (err == 0)
	{
		// Update cache
		OOBase::Guard<OOBase::RWMutex> guard(m_lock);

		m_cache.replace(span,NULL);
		span.m_trans_end_id = trans_id-1;
		m_cache.insert(span,old_block);
	}

	return err;
}

OOKv::BlockStore::Block OOKv::BlockStore::alloc_block(const id_t& trans_id, id_t& block_id, int& err)
{
	Block new_block;

	// Allocate new block from somewhere
	err = 0;

	// Write an alloc record to the journal

	if (err == 0)
	{
		// Add to cache
		OOBase::Guard<OOBase::RWMutex> guard(m_lock);

		BlockSpan span = { block_id, trans_id, 0 };
		m_cache.insert(span,new_block);
	}

	return new_block;
}

int OOKv::BlockStore::do_checkpoint()
{
	// Replay journal, updating the store file!

	// m_current_transaction = last commit

	return 0;
}
