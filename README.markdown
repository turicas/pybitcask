# pybitcask

A pure-Python implementation of
[Basho's bitcask](https://github.com/basho/basho_docs/blob/master/source/data/bitcask-intro.pdf?raw=true)
key-value store.


## License

[GNU General Public License version
3](http://www.gnu.org/licenses/gpl-3.0.html)


## Current Status

This is the current feature implementation status:

- [x] Get
- [x] Set
- [x] Delete
- [ ] Create/read hint file
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

### Concepts

#### Opening a bitcask

- Scan all data files in the directory (to build a new keydir)
- For any data file that has a hint file, use the hint file instead


#### Keydir

The keydir is an in-memory hash table that maps every key in a bitcask to the
information needed to get the actual key value.

    [key] -> (file_id, value_size, value_position, timestamp)


#### Reading a Value

Reading a value does not require more than a single disk seek. It could be
faster than expected because of operating system's read-ahead algorithms.

Simple steps:

- Look for the `key` inside the keydir and take its information (`file_id`,
  `value_size` and `value_position`)
- Seek the file object (probably already open) `file_id` to `value_position`
- Read `value_size` bytes


##### Data File Entry

The data file consists on entries in this format:

    [crc][timestamp][key_size][value_size][key][value]

Where:

- `crc`: 32-bit int with crc32 of `[timestamp]...[value]`
- `timestamp`: 32-bit int with UNIX timestamp
- `key_size`: 16-bit int with key size (in bytes)
- `value_size`: 32-bit int with value size (in bytes)

Some highlights:

- Everything is written in big endian;
- With each write, a new entry is appeneded to the active file (even if it is
  an update or delete operation)
- Deletion is simply a write of a kv-pair with the "value" field filled with a
  "tombstone" special value (the duplication of kv-pair will be removed on the
  merging process);


#### Hint File Entry

Hint files are like a keydir serialized to the filesystem. Each entry is like:

    [timestamp][ksz][value_sz][value_pos][key]


#### Deletion

To be done.


#### Merging Files

Since the main algorithm only appends data we could end up with files bigger
than we actually need. To solve this problem there is a merging mechanism.

- Iterates over all non-active (immutable) data files
- Create new data files with only the last version of each key
- Create new hint files for each new data file


## Possible Improvements

- Store keys sorted when merging (can use binary search on old/fixed files)
- We may increment a special key with some "entropy" value (should be
  incremented by, for example, the size of an deleted item) and when this item
  hits a threshould, the merging process should start automatically.
- Tombstone should be written as 1-bit value on data file
