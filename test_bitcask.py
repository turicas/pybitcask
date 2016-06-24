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

import os
import shutil
import tempfile

import pytest

import bitcask


class TmpDir:
    def setup_method(self, method):
        self.tmpdir = tempfile.mktemp()

    def teardown_method(self, method):
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _path(self, filename):
        return os.path.join(self.tmpdir, filename)


class TestBitcaskPath(TmpDir):

    def test_directory_is_created_if_doesnt_exist(self):
        assert not os.path.exists(self.tmpdir)
        obj = bitcask.Bitcask(path=self.tmpdir)
        assert os.path.exists(self.tmpdir)

    def test_lockfile_exists_but_invalid(self):
        os.mkdir(self.tmpdir)
        lockfile = os.path.join(self.tmpdir, 'bitcask.write.lock')
        fake_pid = 999999
        assert not bitcask._pid_exists(fake_pid)

        with open(lockfile, 'wb') as fobj:
            fobj.write('{} 1.bitcask.data'.format(fake_pid).encode('ascii'))

        obj = bitcask.Bitcask(self.tmpdir)

        assert not os.path.exists(lockfile)

    def test_lockfile_exists_and_valid(self):
        os.mkdir(self.tmpdir)
        lockfile = os.path.join(self.tmpdir, 'bitcask.write.lock')
        pid = os.getpid()
        assert bitcask._pid_exists(pid)

        with open(lockfile, 'wb') as fobj:
            fobj.write('{} 1.bitcask.data'.format(pid).encode('ascii'))

        with pytest.raises(RuntimeError):
            obj = bitcask.Bitcask(self.tmpdir)

    # TODO: lock perms should be 600
    # TODO: check if lock file was created


class TestBitcaskHint(TmpDir):

    # TODO: should check if the hintfile is ok or should it be a separate
    # method?
    # TODO: create test to check CRC
    # TODO: if hintfile is corrupted, should delete it? -- create test

    def test_read_existing_hintfile(self):
        # TODO: we could remove the entry with tombstone for this test to be
        # more simple
        os.mkdir(self.tmpdir)

        # needs to create hintfile *and* datafile because the algorithm
        # searches first for the datafile
        data_filename = self._path('1.bitcask.data')
        hint_filename = self._path('1.bitcask.hint')
        data = b'123456'  # does not matter for this test
        #            timestamp           ksize               vsize               tombstone+vpos                      key
        hintdata = (b'Wh9\xf3'          b'\x00\x08'         b'\x00\x00\x00 '    b'\x00\x00\x00\x00\x00\x00\x00\x00' b'otherkey'
                    b'Wh:P'             b'\x00\x05'         b'\x00\x00\x00)'    b'\x80\x00\x00\x00\x00\x00\x00 '    b'mykey'
                    b'Wh<j'             b'\x00\x06'         b'\x00\x00\x00\x1b' b'\x00\x00\x00\x00\x00\x00\x00I'    b'lalala'
                    b'Wh=q'             b'\x00\x07'         b'\x00\x00\x00\x1c' b'\x00\x00\x00\x00\x00\x00\x00d'    b'lalala2'
                    b'Wh=\x85'          b'\x00\x07'         b'\x00\x00\x00\x1c' b'\x00\x00\x00\x00\x00\x00\x00\x80' b'lalala3'
                    b'\x00\x00\x00\x00' b'\x00\x00'         b'\xcd\x15\xaa\x1e' b'\x7f\xff\xff\xff\xff\xff\xff\xff' b'')

        with open(data_filename, 'wb') as fobj:
            fobj.write(data)
        with open(hint_filename, 'wb') as fobj:
            fobj.write(hintdata)

        obj = bitcask.Bitcask(self.tmpdir)
        assert len(obj._keydir) == 4
        assert len(obj) == len(obj._keydir)

        hint1 = obj._keydir[b'otherkey']
        hint2 = obj._keydir[b'lalala']
        hint3 = obj._keydir[b'lalala2']
        hint4 = obj._keydir[b'lalala3']
        assert hint1.timestamp == 1466448371
        assert hint1.size == 32
        assert hint1.position == 0
        assert hint1.fobj.name == data_filename
        assert hint2.timestamp == 1466449002
        assert hint2.position == 73
        assert hint2.size == 27
        assert hint2.fobj.name == data_filename
        assert hint3.timestamp == 1466449265
        assert hint3.position == 100
        assert hint3.size == 28
        assert hint3.fobj.name == data_filename
        assert hint4.timestamp == 1466449285
        assert hint4.position == 128
        assert hint4.size == 28
        assert hint4.fobj.name == data_filename

    def _make_files(self, hintfile=True, hintdata=None, data=None):
        # Erlang code to generate this data:
        #Bc = bitcask:open("mybitcask", [read_write]).
        #bitcask:put(Bc, <<"mykey">>, <<"myvalue">>).
        #bitcask:put(Bc, <<"mykey2">>, <<"myvalue2">>).
        #bitcask:close(Bc).
        #Bc2 = bitcask:open("mybitcask", [read_write]).
        #bitcask:put(Bc2, <<"mykey2">>, <<"myvalueX">>).
        #bitcask:put(Bc2, <<"mykey">>, <<"myVALUE">>).
        #bitcask:close(Bc2).
        # then, take data from the second .hint and .data files

        if hintdata is None:
            hintdata = (b'Wj\xb63\x00\x06\x00\x00\x00*\x80\x00\x00\x00\x00\x00\x00\x00mykey2'
                        b'Wj\xb63\x00\x06\x00\x00\x00\x1c\x00\x00\x00\x00\x00\x00\x00*mykey2'
                        b'Wj\xb6<\x00\x05\x00\x00\x00)\x80\x00\x00\x00\x00\x00\x00Fmykey'
                        b'Wj\xb6<\x00\x05\x00\x00\x00\x1a\x00\x00\x00\x00\x00\x00\x00omykey'
                        b'\x00\x00\x00\x00\x00\x00G\xabG[\x7f\xff\xff\xff\xff\xff\xff\xff')
        if data is None:
            data = (b'+\xb37\xa8Wj\xb63\x00\x06\x00\x00\x00\x16mykey2bitcask_tombstone2'
                    b'\x00\x00\x00\x01\xa9$E\xc5Wj\xb63\x00\x06\x00\x00\x00\x08mykey2myvalueX'
                    b'\xae7\xf0\xb6Wj\xb6<\x00\x05\x00\x00\x00\x16mykeybitcask_tombstone2'
                    b'\x00\x00\x00\x01\x10_\xbb\x8cWj\xb6<\x00\x05\x00\x00\x00\x07mykeymyVALUE')

        self.data_filename = self._path('1.bitcask.data')
        self.hint_filename = self._path('1.bitcask.hint')
        os.mkdir(self.tmpdir)
        with open(self.data_filename, 'wb') as fobj:
            fobj.write(data)
        if hintfile:
            with open(self.hint_filename, 'wb') as fobj:
                fobj.write(hintdata)

        return hintdata, data

    def test_hintfile_with_tombstones(self):
        self._make_files()

        obj = bitcask.Bitcask(self.tmpdir)
        assert len(obj._keydir) == 2
        assert len(obj) == len(obj._keydir)
        hint1 = obj._keydir[b'mykey']
        hint2 = obj._keydir[b'mykey2']
        assert hint1.timestamp == 1466611260
        assert hint1.position == 111
        assert hint1.size == 26
        assert hint1.fobj.name == self.data_filename
        assert hint2.timestamp == 1466611251
        assert hint2.position == 42
        assert hint2.size == 28
        assert hint2.fobj.name == self.data_filename

    def test_hintfile_from_datafile_without_tombstones(self):
        # following data was generated using bitcask:merge()
        data = (b'\xa9$E\xc5Wj\xb63\x00\x06\x00\x00\x00\x08mykey2myvalueX'
                b'\x10_\xbb\x8cWj\xb6<\x00\x05\x00\x00\x00\x07mykeymyVALUE')
        expected_hint_data = (b'Wj\xb63\x00\x06\x00\x00\x00\x1c\x00\x00\x00\x00\x00\x00\x00\x00mykey2'
                              b'Wj\xb6<\x00\x05\x00\x00\x00\x1a\x00\x00\x00\x00\x00\x00\x00\x1cmykey'
                              b'\x00\x00\x00\x00\x00\x00\xb3v\xfc\xef\x7f\xff\xff\xff\xff\xff\xff\xff')

        self._make_files(data=data, hintfile=False)
        obj = bitcask.Bitcask(self.tmpdir)

        with open(self.hint_filename, 'rb') as fobj:
            created_hint_contents = fobj.read()
        assert created_hint_contents == expected_hint_data

    def test_hintfile_from_datafile_with_tombstones(self):
        expected_hint_data = (b'Wj\xb63'          b'\x00\x06' b'\x00\x00\x00\x1c' b'\x00\x00\x00\x00\x00\x00\x00*'    b'mykey2'
                              b'Wj\xb6<'          b'\x00\x05' b'\x00\x00\x00\x1a' b'\x00\x00\x00\x00\x00\x00\x00o'    b'mykey'
                              b'\x00\x00\x00\x00' b'\x00\x00' b'\x9f\xaf#\x95'    b'\x7f\xff\xff\xff\xff\xff\xff\xff' b'')

        self._make_files(hintfile=False)
        obj = bitcask.Bitcask(self.tmpdir)

        with open(self.hint_filename, 'rb') as fobj:
            created_hint_contents = fobj.read()
        assert created_hint_contents == expected_hint_data

    def test_contains(self):
        # TODO: move to another test class
        self._make_files(hintfile=False)
        obj = bitcask.Bitcask(self.tmpdir)
        assert b'non-ecxiste' not in obj
        assert b'mykey' in obj
        assert b'mykey2' in obj

    def test_get(self):
        # TODO: move to another test class
        self._make_files(hintfile=False)
        obj = bitcask.Bitcask(self.tmpdir)
        with pytest.raises(KeyError) as exception_context:
            obj[b'non-ecxiste']
            import ipdb; ipdb.set_trace()

        assert obj[b'mykey'] == b'myVALUE'
        assert obj[b'mykey2'] == b'myvalueX'

    def test_set_without_overwrite(self):
        # TODO: move to another test class
        self._make_files()
        obj = bitcask.Bitcask(self.tmpdir)
        obj[b'newkey'] = b'newvalue'
        obj[b'anothernewkey'] = b'anothernewvalue'

        assert b'newkey' in obj
        assert b'anothernewkey' in obj

        assert obj[b'newkey'] == b'newvalue'
        assert obj[b'anothernewkey'] == b'anothernewvalue'
        # TODO: check also datafile and hintfile

    def xtest_set_with_overwrite(self):
        # TODO: move to another test class
        assert False  # TODO: create test for tombstone
        self._make_files()
        obj = bitcask.Bitcask(self.tmpdir)
        obj[b'newkey'] = b'newvalue'
        obj[b'anothernewkey'] = b'anothernewvalue'

        assert b'newkey' in obj
        assert b'anothernewkey' in obj

        assert obj[b'newkey'] == b'newvalue'
        assert obj[b'anothernewkey'] == b'anothernewvalue'
        # TODO: check also datafile and hintfile

# TODO: test hintfile update (with CRC) on Bitcask.close()
# TODO: test __del__
# TODO: __init__ should update as dict({'a': 123, 'b': 456})
# TODO: should bitcask respect order on keydir?
# TODO: test set
# TODO: test del
# TODO: test many hint files
# TODO: split test file into many files
# TODO: read all tests starting from src/bitcask.erl:2049 to implement here
# TODO: Add 'sync' (autosync?) option on __init__
# TODO: test .clear
