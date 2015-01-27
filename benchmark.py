#!/usr/bin/env python
# coding: utf-8

import os
import tempfile
import uuid

from time import time

from bitcask import Bitcask
from test_bitcask import random_string


KV_COUNT = 10 ** 4
VALUE_LENGTH = 4096


def main():
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.close()

    kv_store = Bitcask(temp_file.name)

    # Generate random key-value pairs and write them to Bitcask
    total = 0
    time_sum = 0
    keys = []
    for i in range(KV_COUNT):
        key, value = uuid.uuid4().bytes, random_string(VALUE_LENGTH)
        keys.append(key)
        start_time = time()
        kv_store[key] = value
        end_time = time()
        time_sum += end_time - start_time
        total += 1
    print('write average: {} entries/sec'.format(total / time_sum))

    total = 0
    time_sum = 0
    for key in keys:
        start_time = time()
        temp = kv_store[key]
        end_time = time()
        time_sum += end_time - start_time
        total += 1
    print('read average: {} entries/sec'.format(total / time_sum))

    kv_store.close()

    os.unlink(temp_file.name)


if __name__ == '__main__':
    main()
