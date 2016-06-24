# coding: utf-8

import time
import glob
import json
import os
import shutil
import timeit


CODE = '''
import dbm
import hashlib
import tempfile

import bitcask
import redis


def _generate_key_value(counter):
    key = bytes('{:010d}'.format(counter), 'ascii')
    value = (key * 410)[:-4]
    return key, value

TOTAL = ##TOTAL##
testkey, testvalue = _generate_key_value(TOTAL)
keysize = TOTAL * len(testkey)
valuesize = TOTAL * len(testvalue)
totalsize = keysize + valuesize
dbindexes = list(range(15))
print('Total data written: {:7.3f} MB'.format(totalsize / (1024 ** 2)))


def test_bitcask_readwrite():
    path = tempfile.mktemp()
    print('  Bitcask on {}'.format(path))
    bcask = bitcask.Bitcask(path)
    for counter in range(TOTAL):
        key, value = _generate_key_value(counter)
        bcask[key] = value
        assert bcask[key] == value

def test_bitcask_write():
    path = tempfile.mktemp()
    print('  Bitcask on {}'.format(path))
    bcask = bitcask.Bitcask(path)
    for counter in range(TOTAL):
        key, value = _generate_key_value(counter)
        bcask[key] = value

def test_dbm_readwrite():
    path = tempfile.mktemp()
    print('  DBM on {}'.format(path))
    db = dbm.open(path, 'c')
    for counter in range(TOTAL):
        key, value = _generate_key_value(counter)
        db[key] = value
        assert db[key] == value
    db.close()

def test_dbm_write():
    path = tempfile.mktemp()
    print('  DBM on {}'.format(path))
    db = dbm.open(path, 'c')
    for counter in range(TOTAL):
        key, value = _generate_key_value(counter)
        db[key] = value
    db.close()

def test_redis():
    port = dbindexes.pop()
    print('  Redis on {}'.format(port))
    redis_dict = redis.StrictRedis('localhost', port=9999)
    for counter in range(TOTAL):
        redis_dict[key] = value
        assert redis_dict[key] == value
'''

def run(total, timer, iterations=3):
    tests_pythondict = {
            #'Python dict': 'test_pythondict()',
            #'Redis': 'test_redis()',
    }
    tests_dbm = {
            'DBM write': 'test_dbm_write()',
            'DBM readwrite': 'test_dbm_readwrite()',
    }
    tests_bitcask = {
            'Bitcask write': 'test_bitcask_write()',
            'Bitcask readwrite': 'test_bitcask_readwrite()',
    }

    result = {}
    test_code = CODE.replace('##TOTAL##', str(total))

    for test_name, function_call in tests_dbm.items():
        duration = timeit.timeit(function_call, test_code, number=iterations,
                timer=timer)
        print('{} => {}'.format(test_name, duration))
        result[test_name] = duration
        for filename in glob.glob('/tmp/*.db'):
            os.remove(filename)

    result['DBM read'] = result['DBM readwrite'] - result['DBM write']

    for test_name, function_call in tests_bitcask.items():
        duration = timeit.timeit(function_call, test_code, number=iterations,
                timer=timer)
        print('{} => {}'.format(test_name, duration))
        result[test_name] = duration
        for filename in glob.glob('/tmp/tmp*'):
            shutil.rmtree(filename)
    result['Bitcask read'] = result['Bitcask readwrite'] - result['Bitcask write']

    return result

def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('output_filename')
    parser.add_argument('--timer', default='wallclock')
    args = parser.parse_args()
    if args.timer == 'wallclock':
        timer = time.time
    elif args.timer == 'cpu':
        timer = time.process_time
    else:
        raise ValueError('Wrong timer (should be wallclock or cpu)')

    bench = {}
    kv_count = (10, 100, 1000, 10000, 100000, 1000000) #, 2000000, 4000000)
    for runs in kv_count:
        bench[runs] = run(runs, timer)
        print(bench)
        print('')

    plot = {}
    for x, data in bench.items():
        for curva, y in data.items():
            if curva not in plot:
                plot[curva] = []
            plot[curva].append((x, y))
    for a, b in plot.items():
        plot[a] = sorted(b)
    print(plot)
    N = len(list(plot.items())[0])
    bitcask_read = [y for x, y in plot['Bitcask read']]
    bitcask_write = [y for x, y in plot['Bitcask write']]
    dbm_read = [y for x, y in plot['DBM read']]
    dbm_write = [y for x, y in plot['DBM write']]
    labels = [str(x) for x, y in plot['Bitcask read']]
    names = ('bitcask_read', 'bitcask_write', 'dbm_read', 'dbm_write')

    data = {}
    data['xlabels'] = kv_count
    data['curve_data'] = [
            ('dbm read', dbm_read),
            ('bitcask read', bitcask_read),
            ('dbm write', dbm_write),
            ('bitcask write', bitcask_write),
            ]
    with open(args.output_filename, 'w') as fobj:
        json.dump(data, fobj, indent=2)


if __name__ == '__main__':
    main()
