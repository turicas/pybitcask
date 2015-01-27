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

import os
import random
import struct
import tempfile
import time
import unittest
import uuid
import zlib

from bitcask import Bitcask, pack_data


VALUE_LENGTH = 2 ** 10
KV_COUNT = 10 ** 3

def random_string(size=1048576):  # default size is 1MB
    return os.urandom(size)

class TestBitcask(unittest.TestCase):

    files_to_delete = []

    def tearDown(self):
        for filename in self.files_to_delete:
            try:
                os.unlink(filename)
            except OSError:
                pass

    def test_write_one_key_value_pair(self):
        # TODO: should test pack_data
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()
        self.files_to_delete.append(temp_file.name)

        key = b'some key'
        value = b'some value'

        kv_store = Bitcask(temp_file.name)
        kv_store[key] = value
        kv_store.close()

        timestamp = int(time.time())
        key_size = len(key)
        value_size = len(value)
        data = struct.pack('>IHI', timestamp, key_size, value_size) + key + \
               value
        crc = zlib.crc32(data)
        expected_result = struct.pack('>i', crc) + data

        with open(temp_file.name, 'rb') as fobj:
            self.assertEqual(fobj.read(), expected_result)

    def test_write_more_than_one_key_value_pair_at_once(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()
        self.files_to_delete.append(temp_file.name)

        keys_values = [(b'eggs', b'spam'), (b'ham', '42'),
                       (b'monty', b'python'), (b'spam', random_string())]

        packed_data = b''
        kv_store = Bitcask(temp_file.name)
        for key, value in keys_values:
            data, _, _ = pack_data(key, value)
            packed_data += data
            kv_store[key] = value
        kv_store.close()

        with open(temp_file.name, 'rb') as fobj:
            self.assertEqual(fobj.read(), packed_data)

    def test_write_more_than_one_key_value_pair_twice(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()
        self.files_to_delete.append(temp_file.name)

        keys_values = [(b'eggs', b'python'), (b'ham', '42'),
                       (b'monty', b'spam'), (b'spam', random_string())]
        packed_data = b''

        middle = int(len(keys_values) / 2)
        kv_store = Bitcask(temp_file.name)
        for key, value in keys_values[:middle]:
            data, _, _ = pack_data(key, value)
            packed_data += data
            kv_store[key] = value
        kv_store.close()

        kv_store = Bitcask(temp_file.name)
        for key, value in keys_values[middle:]:
            data, _, _ = pack_data(key, value)
            packed_data += data
            kv_store[key] = value
        kv_store.close()

        with open(temp_file.name, 'rb') as fobj:
            self.assertEqual(fobj.read(), packed_data)

    def test_read_keys(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.files_to_delete.append(temp_file.name)
        fobj = temp_file.file

        # Generate random key-value pairs, pack it and save to temp file
        packed_data = b''
        kv_data = {}
        for i in range(KV_COUNT):
            key, value = uuid.uuid4().bytes, random_string(VALUE_LENGTH)
            kv_data[key] = value
            data, _, _ = pack_data(key, value)
            packed_data += data
        fobj.write(packed_data)
        fobj.close()

        # Check every key-value pair using bitcask read API
        kv_store = Bitcask(temp_file.name)
        for key, value in kv_data.items():
            self.assertEqual(kv_store[key], value)

        # test KeyError for non-existing keys
        unexisting_keys = [uuid.uuid4().bytes for x in range(KV_COUNT)]
        for key in unexisting_keys:
            with self.assertRaises(KeyError):
                kv_store[key]

        kv_store.close()

    def test_overwrite_keys(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()
        self.files_to_delete.append(temp_file.name)

        kv_store = Bitcask(temp_file.name)

        # Generate random key-value pairs and write them to Bitcask
        keys = []
        for i in range(KV_COUNT):
            key, value = uuid.uuid4().bytes, random_string(VALUE_LENGTH)
            kv_store[key] = value
            keys.append(key)

        # Rewrite every value
        data = {}
        for key in keys:
            new_value = random_string(VALUE_LENGTH)
            kv_store[key] = new_value
            data[key] = new_value

        # Check every key-value pair using bitcask read API
        for key, value in data.items():
            self.assertEqual(kv_store[key], value)

        kv_store.close()

    def test_len(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()
        self.files_to_delete.append(temp_file.name)

        kv_store = Bitcask(temp_file.name)

        # Generate random key-value pairs and write them to Bitcask
        key_count = random.randint(1, KV_COUNT)
        for i in range(key_count):
            key, value = uuid.uuid4().bytes, random_string(VALUE_LENGTH)
            kv_store[key] = value

        kv_len = len(kv_store)
        self.assertEqual(kv_len, key_count)

        kv_store.close()

    def test_contains(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()
        self.files_to_delete.append(temp_file.name)

        kv_store = Bitcask(temp_file.name)

        # Generate random key-value pairs and write them to Bitcask
        keys = []
        for i in range(KV_COUNT):
            key, value = uuid.uuid4().bytes, random_string(VALUE_LENGTH)
            kv_store[key] = value
            keys.append(key)

        # Test every added key
        for key in keys:
            self.assertIn(key, kv_store)
            self.assertTrue(kv_store.has_key(key))

        # Test other random (non-added) keys
        for x in range(KV_COUNT):
            key = uuid.uuid4().bytes
            self.assertNotIn(key, kv_store)
            self.assertFalse(kv_store.has_key(key))

        kv_store.close()

    def test_iter(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()
        self.files_to_delete.append(temp_file.name)

        kv_store = Bitcask(temp_file.name)

        # Generate random key-value pairs and write them to Bitcask
        keys, values = [], []
        for i in range(KV_COUNT):
            key, value = uuid.uuid4().bytes, random_string(VALUE_LENGTH)
            kv_store[key] = value
            keys.append(key)
            values.append(value)

        self.assertEqual(set(keys), set(kv_store.keys()))
        self.assertEqual(set(keys), set(iter(kv_store)))
        self.assertEqual(set(values), set(kv_store.values()))
        self.assertEqual(set(zip(keys, values)), set(kv_store.items()))

        kv_store.close()

    # TODO: test MAXKEYSIZE (16 bits) and MAXVALSIZE (32 bits)
    # TODO: test MAXOFFSET = 16#7fffffffffffffff (max 63-bit unsigned)

    # TODO: test directory creation with files
    #       - files:
    #         * '{:timestamp}.bitcask.data' (one or more)
    #         * '{:timestamp}.bitcask.hint'
    #         * 'bitcask.write.lock' when writing

    # TODO: test deletion

    # TODO: create hint file
    # Hint file entry:
    #  [<<Tstamp:?TSTAMPFIELD, KeySz:?KEYSIZEFIELD, TotalSz:?TOTALSIZEFIELD,
    #   TombInt:?TOMBSTONEFIELD_V2, Offset:?OFFSETFIELD_V2>>, Key].
    # Where:
    # - TSTAMPFIELD          = 32 bits
    # - KEYSIZEFIELD         = 16 bits
    # - TOTALSIZEFIELD       = 32 bits
    # - TOMBSTONEFIELD_V2    =  1 bit
    # - OFFSETFIELD_V2       = 63 bits
    # Total = 18 bytes (HINT_RECORD_SZ)
    # Also: last (or first?) entry could be a file CRC (check read_crc on
    # bitcask_fileops)

    # TODO: implement merging process (create 'bitcask.merge.lock' when
    #       merging)
