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

#ifndef OOKV_FILE_H_INCLUDED_
#define OOKV_FILE_H_INCLUDED_

#include "config-kv.h"

namespace OOKv
{
	class File
	{
		friend class Directory;

	public:
		File();
		File(const File& rhs);
		~File();
		File& operator = (const File& rhs);

		bool is_open() const;
		int close();

		int write(const void* data, size_t length);
		bool read(void* data, size_t length, int& err);

		template <typename T>
		int write(T val)
		{
			return write(&val,sizeof(T));
		}

		template <typename T>
		bool read(T& val, int& err)
		{
			return read(&val,sizeof(T),err);
		}

		int lock();
		int unlock();

		int length(uint64_t& len) const;
		int tell(uint64_t& pos) const;
		int seek_begin(uint64_t pos);
		int seek_cur(int64_t pos);
		int seek_end(uint64_t pos);
		int truncate(uint64_t len);

		int sync();

	private:
#if defined(_WIN32)
		File(HANDLE handle);
		HANDLE m_handle;
#elif defined(HAVE_UNISTD_H)
		File(int fd);
		int m_fd;
#else
#error Please implement file i/o for your platform
#endif
	};

	class Directory
	{
	public:
		Directory();
		~Directory();

		int open(const char* pszName, bool read_only);

		File open_file(const char* pszName, bool read_only, int& err);
		File create_file(const char* pszName, bool truncate_existing, int& err);

		bool file_exists(const char* pszName);
		int remove_file(const char* pszName);
	};
}

#endif // OOKV_FILE_H_INCLUDED_
