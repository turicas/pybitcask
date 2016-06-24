# pybitcask

A pure-Python (Python3 only) implementation of [Basho's
bitcask](http://basho.com/wp-content/uploads/2015/05/bitcask-intro.pdf)
key-value store.


## License

[GNU General Public License version
3](http://www.gnu.org/licenses/gpl-3.0.html)


## Current Status

This is the current feature implementation status:

- [x] Get
- [x] Set (partially - works ok if key does not exist)
- [ ] Delete
- [x] Create/read hint file
- [ ] Merge old files
- [ ] Rotation of data files

## Concepts

### General

- A Bitcask instance is a directory;
- Only one system process will open a Bitcask instance for writing at a given
  time;
- The Bitcask instance directory consists of many files but only one is the
  "active". The other files are fixed, read-only (there is a rotation process
  to create a new active file when a threshold is exceeded);
- Once a file is closed, either purposefully or due to server exit, it is
  considered immutable and will never be opened for writing again;
- The active file is only written by appending (sequential writes do not
  require disk seeking);
- Deletion is simply a write of a special tombstone value, which will be
  removed on the next merge.
- A Bitcask data file is just a sequence of entries.
- Each data entry has: CRC (of the entire entry), timestamp, key size, value
  size, key and value.
- After the append completes, an in-memory structure called a "keydir" is
  updated.
- A keydir is simply a hash table that maps every key in a Bitcask to a
  fixed-size structure giving the file, offset, and size of the most recently
  written entry for that key.
- When a write occurs, the keydir is atomically updated with the location of
  the newest data.
- Reading a value is simple, and doesn’t ever require more than a single disk
  seek. We look up the key in our keydir, and from there we read the data using
  the file id, position, and size that are returned from that lookup
- The merge process iterates over all non-active (i.e. immutable) files in a
  Bitcask and produces as output a set of data files containing only the "live"
  or latest versions of each present key
- When this is done we also create a "hint file" next to each data file. These
  are essentially like the data files but instead of the values they contain
  the position and size of the values within the corresponding data file
- When a Bitcask is opened it scans all of the data files in a directory in
  order to build a new keydir. For any data file that has a hint file, that
  will be scanned instead for a much quicker startup time.
- Python version will open as read-write
- Erlang operations:
  - `open` (Python's equivalent is `__init__`)
  - `get` (Python's equivalent is `__getitem__`)
  - `put` (Python's equivalent is `__setitem__`)
  - `delete` (Python's equivalent is `__delitem__`)
  - `list_keys` (Python's equivalent is `__iter__`)
  - `fold` (there's no Python equivalence, but can be made easily using
    `__iter__` - it's just a reduce)
  - `merge` (not implemented in Python yet, would be `merge` also)
  - `sync` (not implemented in Python yet, would be `flush`)
  - `close` (Python's equivalent is `close` and `__del__`)


### Concepts

#### Opening a bitcask

- Scan all data files in the directory (to build a new keydir)
- For any data file that has a hint file, use the hint file instead
- Check the CRC for the hintfile before reading it

#### Keydir

The keydir is an in-memory hash table that maps every key in a bitcask to the
information needed to get the actual key value.

    [key] -> (file_id, entry_size, entry_position, timestamp)

Where "entry" is the complete entry (crc, timestamp, keysize, valuesize, key
and value) in the datafile.


#### Reading a Value

Reading a value does not require more than a single disk seek. It could be
faster than expected because of operating system's read-ahead algorithms.

Simple steps:

- Look for the `key` inside the keydir and take hint information (`file_id`,
  `entry_position` and `entry_size`)
- Seek the file object `file_id` to `entry_position`
- Read `entry_size` bytes
- Unpack the data read


##### Data File Entry

The data file consists on entries in this format:

    [crc][timestamp][key_size][value_size][key][value]

Where:

- `crc`: 32-bit int with crc32 of
  `[timestamp][key_size][value_size][key][value]`
- `timestamp`: 32-bit int with UNIX timestamp
- `key_size`: 16-bit int with key size (in bytes)
- `value_size`: 32-bit int with value size (in bytes)

Some details:

- Everything is written in big endian;
- With each write, a new entry is appeneded to the active file (even if it is
  an update or delete operation)
- Deletion is simply a write of a kv-pair with the "value" field filled with a
  "tombstone" special value (the duplication of kv-pair will be removed on the
  merging process).


#### Hint File Entry

Hint files are like a keydir serialized to the filesystem. Each entry is like:

    [timestamp][key_size][entry_size][entry_position][key]


#### Deletion

To be done.


#### Merging Files

Since the main algorithm only appends data we could end up with files bigger
than we actually need. To solve this problem there is a merging mechanism.

- Iterates over all non-active (immutable) data files
- Create new data files with only the last version of each key
- Create new hint files for each new data file


## Possible Improvements

- Store keys sorted when merging (could be useful in some scenarios to optimize
  search or map-reduce)
- We may increment a special key with some "entropy" value (should be
  incremented by, for example, the size of an deleted item) and when this item
  hits a threshould, the merging process should start automatically.
- Tombstone should be written as 1-bit value on data file


## Running Basho's Bitcask (in Erlang)

```
git clone https://github.com/basho/bitcask.git
cd bitcask
./rebar get-deps
./rebar compile
./rebar shell
```

Now play with it:

```erlang
Bc = bitcask:open("/tmp/mycask", [read_write]).
bitcask:put(Bc, <<"otherkey">>, <<"othervalue">>).
bitcask:delete(Bc, <<"mykey">>).
```
