# coding: utf-8

# Copyright 2015-2016 Álvaro Justen
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

'''This module implements Basho's Bitcask key-value store in Python

It provides the class `Bitcask`, which has the `collections.abc.MutableMapping`
interface.
'''

import binascii
import glob
import io
import os
import struct
import time

from collections import namedtuple
from collections.abc import MutableMapping

import psutil

#FIRST_STRUCT = Struct('>IHI')
#SECOND_STRUCT = Struct('>i')
#HEADER_SIZE = 4 + 4 + 2 + 4  # crc + timestamp + key_size + value_size
BITCASK_WRITE_LOCK = 'bitcask.write.lock'
BITCASK_DATA = '{}.bitcask.data'
BITCASK_HINT = '{}.bitcask.hint'
STRUCT_HINT = struct.Struct('>IHIQ')
STRUCT_DATA = struct.Struct('>IIHI')
STRUCT_INT16 = struct.Struct('>H')
STRUCT_INT32 = struct.Struct('>I')
STRUCT_POS = struct.Struct('>Q')
HINTFILE_END = STRUCT_POS.unpack(b'\x7f\xff\xff\xff\xff\xff\xff\xff')[0]
DATA_NULL = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
TOMBSTONE_PREFIX = b'bitcask_tombstone'
MAX_KEYSIZE = 2 ** 16
MAX_VALUESIZE = 2 ** 63
Hint = namedtuple('Hint', ['fobj', 'position', 'size', 'timestamp'])


def _pid_exists(pid):
    'Check if some PID is running on the system'

    return pid in psutil.pids()


class Bitcask(MutableMapping):
    """Implements Bitcask based on Basho's source code (in Erlang)

    Field sizes: (from include/bitcask.hrl):

    - crc: unsigned int, 32 bits (4 bytes), struct format: I
    - timestamp: unsigned int, 32 bits (4 bytes), struct format: I
    - key_size: unsigned int, 16 bits (2 bytes), struct format: H
    - value_size: unsigned int, 32 bits (4 bytes), struct format: I

    Erlang uses big endian by default, so our struct format starts with '>'
    """

    def __init__(self, path, sync=True):
        self.sync = True
        self._active_data = None
        self._active_hint = None
        self._bitcask_path = path
        self._files = set()
        self._keydir = {}

        if not os.path.exists(path):
            os.mkdir(path)
        else:
            lockfile = os.path.join(path, BITCASK_WRITE_LOCK)
            if os.path.exists(lockfile):
                with open(lockfile, 'rt') as fobj:
                    lockdata = fobj.read()
                pid, filename = lockdata.split()
                if not _pid_exists(int(pid)):  # invalid lock file
                    os.remove(lockfile)
                else:
                    # TODO: change this behaviour to allow read-only mode
                    raise RuntimeError('Bitcask is locked by process {}'
                            .format(pid))

        self._open_files()

    def _path(self, filename):
        return os.path.join(self._bitcask_path, filename)

    def _load_immutable_file(self, filename):
        fobj = open(filename, 'rb')
        hintfilename = filename.replace('.data', '.hint')
        if os.path.exists(hintfilename):
            self._read_hintfile(hintfilename, fobj)
        else:
            self._create_hintfile_from_datafile(fobj, hintfilename)
        return fobj

    def _write_hintfile_crc(self):
        # TODO: test
        self._active_hint.seek(0)
        data = self._active_hint.read()
        self._active_hint.write(STRUCT_HINT.pack(0, 0, binascii.crc32(data), HINTFILE_END))

    def _read_hintfile(self, filename, datafobj):
        fobj = open(filename, 'rb')
        self._files.add(fobj)

        # check CRC (last entry of the file)
        filedata = fobj.read()
        hintdata, crcdata = filedata[:-18], filedata[-18:]
        _, _, crc, _ = STRUCT_HINT.unpack(crcdata)
        checked_crc = binascii.crc32(hintdata)
        if crc != checked_crc:
            # This hintfile is corrupted (or was not completely written,
            # because of CRC mismatch), so we're going to write a new one.

            # TODO: logger.warning('corrupted hint file, creating another')
            self._files.remove(fobj)
            self._create_hintfile_from_datafile(datafobj, filename)
            return

        # TODO: should remove CRC/truncate file? (only for active hint files)
        hintbytes = io.BytesIO(hintdata)
        data = hintbytes.read(18)
        while data:
            # timestamp, keysize, valuesize, valuepos: 4 bytes unsigned int + 2
            # bytes unsinged int + 4 bytes unsigned int + 8 bytes unsigned long
            # long
            timestamp, ksize, entry_size, vinfo = STRUCT_HINT.unpack(data)
            tombstone = (vinfo >> 63) == 1
            # remove first bit (tombstone) and pack again
            new_data = STRUCT_INT16.unpack(data[10:12])[0] & 0b0111111111111111
            pos_packed = STRUCT_INT16.pack(new_data) + data[12:18]
            entry_position = STRUCT_POS.unpack(pos_packed)[0]

            key = hintbytes.read(ksize)
            if not tombstone:
                self._keydir[key] = Hint(fobj=datafobj,
                                         position=entry_position,
                                         size=entry_size,
                                         timestamp=timestamp)
            data = hintbytes.read(18)

    def _create_hintfile_from_datafile(self, datafobj, hintfilename):
        '''Create a new hint file based on a data file

        Some reasons to completely read a datafile:
        - Create a hintfile
        - Check all CRCs
        - Load key-value pairs "manually" (instead of using the keydir concept)
        '''

        # TODO: what about whence?
        # TODO: what about DATA_NULL?
        hintio = io.BytesIO()
        datafobj.seek(0)
        data = datafobj.read(14)
        while data:
            # crc, timestamp, keysize, valuesize: 4 + 4 + 2 + 4
            crc, timestamp, key_size, value_size = STRUCT_DATA.unpack(data)
            position = datafobj.tell() - 14
            key = datafobj.read(key_size)
            value = datafobj.read(value_size)
            entry_size = datafobj.tell() - position
            checked_crc = binascii.crc32(data[4:] + key + value)
            if crc != checked_crc:
                # It is important to check CRC here since we're creating the
                # hintfile based on this datafile, so to the hintfile to be
                # correct we need to check all datafile's CRCs.

                # TODO: do something else (maybe just go to the next entry)
                raise RuntimeError('CRC error')

            # if it's a tombstone, just ignore it
            if not value.startswith(TOMBSTONE_PREFIX):
                self._keydir[key] = Hint(fobj=datafobj,
                                         position=position,
                                         size=entry_size,
                                         timestamp=timestamp)
                hintio.write(STRUCT_HINT.pack(timestamp,
                                              key_size,
                                              entry_size,
                                              position))
                hintio.write(key)

            data = datafobj.read(14)

        hintio.seek(0)
        hintdata = hintio.read()
        hintcrc = STRUCT_HINT.pack(0,  # timestamp
                                   0,  # key size
                                   binascii.crc32(hintdata),  # value size
                                   HINTFILE_END)  # (value) position
        with open(hintfilename, 'wb') as hintfobj:
            hintfobj.write(hintdata)
            hintfobj.write(hintcrc)

    def _open_files(self):
        'Open immutable and active files'

        # open immutable files for reading
        filenames = glob.glob(self._path(BITCASK_DATA.format('*')))
        for filename in filenames:  # TODO: assert order
            self._files.add(self._load_immutable_file(filename))

        # create next active file
        next_fileid = 1
        if filenames:
            next_fileid = max(int(os.path.basename(filename).split('.')[0])
                              for filename in filenames)
        active_data_filename = self._path(BITCASK_DATA.format(next_fileid))
        active_hint_filename = self._path(BITCASK_HINT.format(next_fileid))

        # TODO: may use mmap on these files
        self._active_data = open(self._path(BITCASK_DATA.format(next_fileid)),
                                 'a+b')
        self._active_hint = open(self._path(BITCASK_HINT.format(next_fileid)),
                                 'a+b')
        self._files.add(self._active_data)
        self._files.add(self._active_hint)

    def __delitem__(self, item):
        # TODO: check max keysize?
        raise NotImplementedError()

    def __setitem__(self, key, value):
        key_size = len(key)
        value_size = len(value)
        if key_size > MAX_KEYSIZE:
            # TODO: Test
            raise ValueError('Key is greater than {}'.format(MAX_KEYSIZE))
        elif value_size > MAX_VALUESIZE:
            # TODO: Test
            raise ValueError('Value is greater than {}'.format(MAX_VALUESIZE))
        elif value.startswith(TOMBSTONE_PREFIX):
            # TODO: Test
            raise ValueError('Value cannot start with "{}"'.format(TOMBSTONE_PREFIX))
        else:
            # TODO: if key already exists on hintfile or datafile, write
            # tombstone (if key in self._keydir: ...)

            timestamp = int(time.time())
            entry_size = 14 + key_size + value_size  # 14 bytes = data header
            data_entry = STRUCT_DATA.pack(0, timestamp, key_size, value_size)
            crc = binascii.crc32(data_entry[4:])
            crc = binascii.crc32(key, crc)
            crc = STRUCT_INT32.pack(binascii.crc32(value, crc))
            entry_position = self._active_data.seek(0, 2)
            self._active_data.write(crc)
            self._active_data.write(data_entry[4:])
            self._active_data.write(key)
            self._active_data.write(value)
            if self.sync:
                self._active_data.flush()

            hint_entry = STRUCT_HINT.pack(timestamp,
                                          key_size,
                                          entry_size,
                                          entry_position)
            self._active_hint.seek(0, 2)
            # TODO: if we maintain the hint file cursor only at the end, we may
            # not need this seek
            self._active_hint.write(hint_entry)
            # we don't need to flush hint file on every write, since it will be
            # flushed sometime in the future by the OS and if it's corrupted
            # somehow, we can just create another one based on data file.

            self._keydir[key] = Hint(fobj=self._active_data,
                                     position=entry_position,
                                     size=entry_size,
                                     timestamp=timestamp)

    def __contains__(self, key):
        return key in self._keydir

    def __getitem__(self, key):
        hint = self._keydir[key]
        hint.fobj.seek(hint.position)
        data = hint.fobj.read(hint.size)
        _, _, key_size, _ = STRUCT_DATA.unpack(data[:14])
        return data[14 + key_size:]

        # TODO: should check CRC on every read or create another operation to
        # fully scan the datafiles for corruption? or both?
        # Current opinion on this: we don't need to check, since we check the
        # hintfile CRC when opening the cask (or check every key-value CRC if
        # the hintfile is corrupted or does not exist, to create a new one)
        #if binascii.crc32(data[4:]) != crc:
        #    raise ValueError('CRC error')
        #else:
        #    return data[14 + key_size:]

    def __len__(self):
        return len(self._keydir)

    def __iter__(self):
        # TODO: test
        return (key for key in self._keydir)

    def close(self):
        # TODO: remove lock file
        # TODO: should be the same as __del__
        for fobj in self._files:
            fobj.close()

    def __del__(self):
        # TODO: test
        # TODO: calculate active hintfile CRC incrementally
        self._write_hintfile_crc()
        self.close()
