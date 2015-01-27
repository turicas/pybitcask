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

### Reading a Key-Value Pair

To be done.

### Data File Entry

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


### Hint File Entry

To be done.

### Deletion

To be done.

### Merging Files

To be done.

### Possible Improvements

To be done.
