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

namespace OOKv
{
	class File
	{
	public:
		File();
		~File();

		static bool exists(const char* pszName);
		static int remove(const char* pszName);

		int open(const char* pszName, bool read_only);
		int create(const char* pszName, bool truncate_existing);
		int close();

		int write(const void* data, size_t length);
		int read(void* data, size_t length);

		template <typename T>
		int write(T val)
		{
			return write(&val,sizeof(T));
		}

		template <typename T>
		int read(T& val)
		{
			return read(&val,sizeof(T));
		}

		uint64_t tell() const;
		int seek_begin(uint64_t pos);
		int seek_cur(int64_t pos);
		int seek_end(uint64_t pos);

		int sync();
	};
}

#endif // OOKV_FILE_H_INCLUDED_
