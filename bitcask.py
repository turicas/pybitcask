# coding: utf-8

# Copyright 2015 Álvaro Justen
#
# This file is part of pybitcask.
# You can get more information at: https://github.com/turicas/pybitcask
#
# pybitcask is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or # (at your option) any later
# version.
#
# pybitcask is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with pybitcask. If not, see <http://www.gnu.org/licenses/>.

from collections import MutableMapping, namedtuple
from struct import Struct
from time import time
from zlib import crc32


FIRST_STRUCT = Struct('>IHI')
SECOND_STRUCT = Struct('>i')
HEADER_STRUCT = Struct('>iIHI')
HEADER_SIZE = 4 + 4 + 2 + 4  # crc + timestamp + key_size + value_size
Hint = namedtuple('Hint', ['file_id', 'value_size', 'value_position',
                           'timestamp'])


def pack_data(key, value):
    """Pack a key-value pair to store on the data/log file

    Format: [crc][timestamp][ksz][value_sz][key][value]

    Each field having the following sizes (from include/bitcask.hrl):

    - crc: signed int, 32 bits (4 bytes), struct format: i
    - timestamp: unsigned int, 32 bits (4 bytes), struct format: I
    - key_size: unsigned int, 16 bits (2 bytes), struct format: H
    - value_size: unsigned int, 32 bits (4 bytes), struct format: I

    Erlang uses big endian by default, so our struct format starts with '>'

    Struct formats we use:
    - H = unsigned short int (2 bytes)
    - i = int (4 bytes)
    - I = unsigned int (4 bytes)
    """

    value_size, timestamp = len(value), int(time())
    data = FIRST_STRUCT.pack(timestamp, len(key), value_size) + key + value
    entry = SECOND_STRUCT.pack(crc32(data)) + data
    return entry, value_size, timestamp

def read_hint_from_data_file(fobj):
    header = fobj.read(HEADER_SIZE)
    if header == '':
        raise RuntimeError()

    # TODO: test corrupted data (struct can't unpack)
    crc, timestamp, key_size, value_size = HEADER_STRUCT.unpack(header)
    key = fobj.read(key_size)
    if header == '':
        raise RuntimeError()

    value_position = fobj.tell()
    fobj.seek(value_size, 1)  # whence=1 => relative position = current
    # TODO: check CRC
    return timestamp, value_position, value_size, key


class Bitcask(MutableMapping):
    def __init__(self, filename):
        # TODO: add read-write option (default = read-only)
        # TODO: add flush option (default = ?)
        # TODO: a bitcask should be an entire directory, not a file
        self.__fobj = open(filename, 'a+b')
        self.flush = True
        self.__make_keydir()

    def __make_keydir(self):
        """Traverses active file to create the in-RAM keydir"""
        fobj = self.__fobj
        fobj.seek(0)
        file_id = fobj.name
        self.__keydir = {}

        while True:
            try:
                timestamp, value_position, value_size, key = \
                        read_hint_from_data_file(fobj)
            except RuntimeError:
                break
            else:
                self.__keydir[key] = Hint(file_id, value_size, value_position,
                                          timestamp)
        fobj.seek(0)

    def __setitem__(self, key, value):
        """Append pair entry to active file, update keydir and hint file"""

        fobj = self.__fobj
        data, value_size, timestamp = pack_data(key, value)

        fobj.seek(0, 2)  # whence=2 => relative position = EOF
        fobj.write(data)
        if self.flush:
            self.__fobj.flush()

        value_position = self.__fobj.tell() - value_size
        hint = Hint(self.__fobj.name, value_size, value_position, timestamp)
        self.__keydir[key] = hint

        # TODO: update hint file

    def __getitem__(self, key):
        """Get hint from keydir and load data from disk"""

        hint = self.__keydir[key]
        fobj = self.__fobj
        fobj.seek(hint.value_position)
        return fobj.read(hint.value_size)

    def __delitem__(self, key):
        raise NotImplementedError()

    def clear(self):
        raise NotImplementedError()

    def __len__(self):
        return len(self.__keydir)

    def __iter__(self):
        return (key for key in self.__keydir)

    def __contains__(self, key):
        return key in self.__keydir

    has_key = __contains__

    def close(self):
        """Close active file"""

        self.__fobj.flush()
        self.__fobj.close()
