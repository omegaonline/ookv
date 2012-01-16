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

#include "config-kv.h"

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
			Begin = 0,
			Alloc,
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

		int load(const char* path, bool read_only);

		virtual Block load_block(const id_t& block_id, id_t& start_trans_id, int& err);

		id_t begin_read_transaction(int& err);
		int end_read_transaction(const id_t& trans_id);

		Block get_block(const id_t& block_id, const id_t& trans_id, int& err);

		int validate_checkpoint_file(File& file);

		// Persistent data
		id_t m_last_transaction;
		id_t m_first_transaction;
		id_t m_free_list_head_block;

		// Volatile data - controlled by m_lock
		OOBase::RWMutex                     m_lock;
		OOBase::Set<id_t>                   m_read_transactions;
		OOBase::TableCache<BlockSpan,Block> m_cache;

		// Volatile data - controlled by m_journal lock
		OOBase::SpinLock                    m_journal_lock;
		File                                m_journal_file;
		uint64_t                            m_journal_start;

		// Uncontrolled data - init'd at load()
		Directory                           m_store_directory;
		File                                m_store_file;
		OOBase::String                      m_store_name;

	private:
		int apply_journal(Block& block, const BlockSpan& from, const id_t& to);
	};

	class BlockStoreRO : public BlockStoreBase
	{
	public:
		int open_i(const char* path);

		Block load_block(const id_t& block_id, id_t& start_trans_id, int& err);

		id_t begin_write_transaction(int& err, const OOBase::Countdown& countdown = OOBase::Countdown()) { err=EROFS; return 0;}
		int commit_write_transaction(const id_t& trans_id) { return EROFS; }
		void rollback_write_transaction(const id_t& trans_id) {}

		int checkpoint(const OOBase::Countdown& countdown = OOBase::Countdown()) { return EROFS; }

		int update_block(const id_t& block_id, const id_t& trans_id, Block block) { return EROFS; }
		id_t alloc_block(const id_t& trans_id, Block& block, int& err) { err=EROFS; return 0; }
		int free_block(const id_t& block_id, const id_t& trans_id) { return EROFS; }

	private:
		File m_checkpoint_file;
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
		int apply_checkpoint(File& checkpoint_file, bool validate);
	};

	template <typename T>
	OOKv::BlockStore* open_t(const char* path, int& err)
	{
		T* store = new (std::nothrow) T();
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

OOKv::BlockStore* OOKv::BlockStore::open(const char* path, bool read_only, int& err)
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

int BlockStoreBase::load(const char* path, bool read_only)
{
	// Build the relative filenames...
	OOBase::LocalString dir_name, journal_name, checkpoint_name;
	int err = OOBase::Paths::SplitDirAndFilename(path,dir_name,m_store_name);
	if (err == 0)
		err = journal_name.concat(m_store_name.c_str(),".journal");
	if (err != 0)
		return err;

	// Open the parent directory
	if ((err = m_store_directory.open(dir_name.c_str(),read_only)) != 0)
		return err;

	// Open store
	m_store_file = m_store_directory.open_file(m_store_name.c_str(),read_only,err);
	if (err != 0)
		return err;

	// Check for journal file
	if (m_store_directory.file_exists(journal_name.c_str()))
		m_journal_file = m_store_directory.open_file(journal_name.c_str(),read_only,err);
	else if (!read_only)
		m_journal_file = m_store_directory.create_file(journal_name.c_str(),false,err);

	// Init m_journal_start from the store m_first_transaction
	void* TODO;

	return err;
}

OOKv::id_t BlockStoreBase::begin_read_transaction(int& err)
{
	OOBase::Guard<OOBase::RWMutex> guard(m_lock);

	err = m_read_transactions.insert(m_last_transaction);
	if (err != 0)
		return 0;

	return m_last_transaction;
}

int BlockStoreBase::end_read_transaction(const id_t& trans_id)
{
	OOBase::Guard<OOBase::RWMutex> guard(m_lock);

	return m_read_transactions.remove(trans_id);
}

BlockStore::Block BlockStoreBase::load_block(const id_t& block_id, id_t& start_trans_id, int& err)
{
	// Load the block data from the store
	void* TODO;

	return Block();
}

BlockStore::Block BlockStoreBase::get_block(const id_t& block_id, const id_t& trans_id, int& err)
{
	if (trans_id > m_last_transaction || block_id == 0 || trans_id == 0)
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

int BlockStoreBase::validate_checkpoint_file(File& file)
{
	void* TODO;

	return 0;
}

int BlockStoreRO::open_i(const char* path)
{
	int err = load(path,true);
	if (err != 0)
		return err;

	// Open checkpoint in case the BlockStore crashed during a checkpoint
	OOBase::LocalString checkpoint_name;
	if ((err = checkpoint_name.concat(m_store_name.c_str(),".checkpoint")) != 0)
		return err;

	if (m_store_directory.file_exists(checkpoint_name.c_str()))
	{
		m_checkpoint_file = m_store_directory.open_file(checkpoint_name.c_str(),true,err);

		// Validate checkpoint file...
		err = validate_checkpoint_file(m_checkpoint_file);
	}

	return err;
}

BlockStore::Block BlockStoreRO::load_block(const id_t& block_id, id_t& start_trans_id, int& err)
{
	Block block = BlockStoreBase::load_block(block_id,start_trans_id,err);
	if (err == 0 && block && m_checkpoint_file.is_open())
	{
		// Play through checkpoint file in case we have a read-only crashed store

		void* TODO;
	}

	return block;
}

BlockStoreRW::BlockStoreRW() : BlockStoreBase(),
		m_write_inprogress(false)
{
}

BlockStoreRW::~BlockStoreRW()
{
	if (checkpoint() == 0)
	{
		if (m_journal_file.is_open())
		{
			m_journal_file.unlock();
			m_journal_file.close();
		}

		// Remove the journal file
		OOBase::LocalString journal_name;
		if (journal_name.concat(m_store_name.c_str(),".journal") == 0)
			m_store_directory.remove_file(journal_name.c_str());
	}
}

int BlockStoreRW::open_i(const char* path)
{
	int err = load(path,false);
	if (err != 0)
		return err;

	// Attempt to gain exclusive lock on journal file
	err = m_journal_file.lock();
	if (err != 0)
		return err;

	// Apply checkpoint in case the BlockStore crashed during a checkpoint
	OOBase::LocalString checkpoint_name;
	if ((err = checkpoint_name.concat(m_store_name.c_str(),".checkpoint")) != 0)
		return err;

	if (m_store_directory.file_exists(checkpoint_name.c_str()))
	{
		File checkpoint_file = m_store_directory.open_file(checkpoint_name.c_str(),true,err);
		if (err != 0)
			return err;

		if ((err = apply_checkpoint(checkpoint_file,true)) != 0)
			return err;

		checkpoint_file.close();

		m_store_directory.remove_file(checkpoint_name.c_str());
	}

	// Do a checkpoint and ignore errors, the store is safe anyway
	do_checkpoint();

	return err;
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

	uint64_t length = 0;
	if (!m_log.write(static_cast<uint64_t>(LogRecord::Begin)) || !m_log.write(m_last_transaction+1) || !m_log.write(length))
	{
		err = m_log.last_error();
		return 0;
	}

	m_write_inprogress = true;

	return m_last_transaction+1;
}

int BlockStoreRW::commit_write_transaction(const id_t& trans_id)
{
	OOBase::Guard<OOBase::Condition::Mutex> guard(m_write_lock);

	if (!m_write_inprogress || trans_id != m_last_transaction+1)
		return EACCES;

	int err = 0;

	// Write a commit record to the log
	if (!m_log.write(static_cast<uint64_t>(LogRecord::Commit)))
	{
		err = m_log.last_error();
	}
	else
	{
		// Make sure we update the length marker before we start
		m_log.replace(static_cast<uint64_t>(m_log.buffer()->length()-12),8);

		OOBase::Guard<OOBase::SpinLock> journal_guard(m_journal_lock);

		// Seek journal to end
		if ((err = m_journal_file.seek_end(0)) != 0)
		{
			// Get journal position
			uint64_t start_pos = 0;
			if ((err = m_journal_file.tell(start_pos)) == 0)
			{
				// Write the log to the journal
				if ((err = m_journal_file.write(m_log.buffer()->rd_ptr(),m_log.buffer()->length()) == 0))
				{
					// Sync the journal
					if ((err = m_journal_file.sync()) == 0)
					{
						uint64_t journal_len = 0;
						m_journal_file.length(journal_len);

						// Check for checkpoint
						if (trans_id % s_checkpoint_interval == 0 || journal_len > 0x40000000)
							err = do_checkpoint();

						if (err == 0)
							m_last_transaction = trans_id;
					}
				}

				if (err != 0)
				{
					// Reset journal file to start_pos
					int err2 = m_journal_file.seek_begin(start_pos);
					if (err2 == 0)
						err2 = m_journal_file.truncate(start_pos);

					if (err2 != 0)
						err = err2;
				}
			}
		}
	}

	m_log.reset();

	m_write_inprogress = false;
	m_write_condition.signal();

	return 0;
}

void BlockStoreRW::rollback_write_transaction(const id_t& trans_id)
{
	OOBase::Guard<OOBase::Condition::Mutex> guard(m_write_lock);

	if (m_write_inprogress && trans_id == m_last_transaction+1)
	{
		// Discard log contents
		m_log.reset();

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
	if (!m_write_inprogress || trans_id != m_last_transaction+1)
		return EACCES;

	if (block_id == 0)
		return EINVAL;

	int err = 0;
	Block prev_block = get_block(block_id,trans_id-1,err);
	if (err != 0)
		return err;

	// Write a diff block to the log
	if (!m_log.write(static_cast<uint64_t>(LogRecord::Diff)) ||
			!m_log.write(block_id))
	{
		return m_log.last_error();
	}

	const char* prev_data = static_cast<const char*>(static_cast<const void*>(prev_block));
	const char* data = static_cast<const char*>(static_cast<const void*>(block));

	// Write the diff of old_block -> block to the log
	for (size_t pos = 0; pos < s_block_size;)
	{
		uint16_t marker;

		for (marker = 0;prev_data[pos] == data[pos] && pos < s_block_size;++pos)
			++marker;

		if (marker != 0 && !m_log.write(marker))
			return m_log.last_error();

		for (marker = 0;prev_data[pos] != data[pos] && pos < s_block_size;++pos)
			++marker;

		if (marker != 0)
		{
			// Write the changed bytes
			if (!m_log.write(marker | 0x8000) || !m_log.write(data + (pos-marker),marker))
				return m_log.last_error();
		}
	}

	// Watch out for very big transactions!
	if (m_log.buffer()->length() > (0x8000000000000000ull - 12))
		return E2BIG;

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
	if (!m_write_inprogress || trans_id != m_last_transaction+1)
		return EACCES;

	if (block_id == 0)
		return EINVAL;

	// Watch out for very big transactions!
	if (m_log.buffer()->length() > (0x8000000000000000ull - 12 - 8))
		return E2BIG;

	// Write a free block record to the log
	if (!m_log.write(static_cast<uint64_t>(LogRecord::Free)) ||
			!m_log.write(block_id))
	{
		return m_log.last_error();
	}

	return 0;
}

OOKv::id_t BlockStoreRW::alloc_block(const id_t& trans_id, Block& block, int& err)
{
	// This is not a 100% race-safe check, but it will help!
	if (!m_write_inprogress || trans_id != m_last_transaction+1)
	{
		err = EACCES;
		return 0;
	}

	// Watch out for very big transactions!
	if (m_log.buffer()->length() > (0x8000000000000000ull - 12 - 8))
	{
		err = E2BIG;
		return 0;
	}

	// Allocate new block from somewhere
	void* TODO;

	id_t block_id = 1;

	// Write an alloc record to the log
	if (!m_log.write(static_cast<uint64_t>(LogRecord::Alloc)) ||
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
	id_t earliest_read_transaction = m_first_transaction;

	// Create checkpoint file
	OOBase::LocalString checkpoint_name;
	int err = checkpoint_name.concat(m_store_name.c_str(),".checkpoint");
	if (err != 0)
		return err;

	File checkpoint_file = m_store_directory.create_file(checkpoint_name.c_str(),true,err);
	if (err != 0)
		return err;

	// Acquire the journal lock
	OOBase::Guard<OOBase::SpinLock> journal_guard(m_journal_lock);

	// Play forward journal to earliest_read_transaction
	if ((err = m_journal_file.seek_begin(m_journal_start)) != 0)
	{
		while (err == 0)
		{
			uint64_t op = 0;
			if (m_journal_file.read(op,err))
			{
				if (op != LogRecord::Commit)
					err = EINVAL;
				else
				{
					id_t trans_id = 0;
					uint64_t next_commit = 0;
					if (!m_journal_file.read(trans_id,err) || !m_journal_file.read(next_commit,err))
					{
						// We must have an id and a next commit!
						if (err == 0)
							err = EINVAL;
					}
					else
					{
						// Get the earliest transaction
						OOBase::ReadGuard<OOBase::RWMutex> read_guard(m_lock);

						id_t earliest_read_transaction = m_last_transaction;
						if (!m_read_transactions.empty())
							earliest_read_transaction = *m_read_transactions.at(0);

						read_guard.release();

						// If we have reached the start of an in-progress read transaction, stop
						if (trans_id >= earliest_read_transaction)
							break;

						// If we are reading an old journal, skip onwards
						if (trans_id < m_first_transaction)
							err = m_journal_file.seek_cur(static_cast<int64_t>(next_commit));
						else
						{
							// Apply each record...
							void* TODO;
						}
					}
				}
			}
		}

		if (err == 0)
		{
			// Now write the 0 block changes...

			// Now write some kind of checksum or hash!

			// Sync checkpoint file
			if ((err = checkpoint_file.sync()) == 0)
			{
				// Play forward checkpoint file, writing each block to store file
				err = apply_checkpoint(checkpoint_file,false);
			}
		}
	}

	// Delete checkpoint file
	checkpoint_file.close();
	m_store_directory.remove_file(checkpoint_name.c_str());

	if (err == 0)
	{
		m_first_transaction = earliest_read_transaction;

		// See if we can truncate the file, and reset m_journal_start
		if (m_first_transaction == m_last_transaction && m_journal_file.truncate(0) == 0)
			m_journal_start = 0;
		else
			m_journal_file.tell(m_journal_start);
	}

	return err;
}

int BlockStoreRW::apply_checkpoint(File& checkpoint_file, bool validate)
{
	// Playback checkpoint file, updating store file

	int err = checkpoint_file.seek_begin(0);
	if (err != 0)
		return err;

	// Check the start of the checkpoint file to see if it has a matching ID
	if (validate)
	{
		if ((err = validate_checkpoint_file(checkpoint_file)) != 0)
			return err;
	}

	for (;;)
	{
		id_t block_id;
		if (!checkpoint_file.read(block_id,err))
			break;

		void* TODO; // Fix the byte_swap!

		block_id = OOBase::byte_swap(block_id);
		if (block_id == 0)
		{
			// Special size for first block
			void* TODO;
		}
		else
		{
			void* TODO;
		}
	}

	// Sync store file
	if (err == 0)
		err = m_store_file.sync();

	return err;
}
