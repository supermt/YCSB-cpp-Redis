# YCSB-CPP for Redis

This repo add supports of Redis in YCSB-CPP (written by LEE)

# Usage:

```
./ycsb -run -db redis -P workloads/workloada -P redis/cluster.prop -s
```

# Slot test

We tend to skew the input according to the following three prefix, and make the
two prefix stores in the first slot, in the second slot, and
nothing in the third one.

## Prefix

- {Astartes} slot 3246 first machine
- {UltraMarine} slot 7797 second machine
- {DarkAngel} (crc16 = 7e24 HEX) mod 16385 = 15907
- {DeathGuard} 11166 Third Machine

## About the `rebalance` command in Redis

This command will only re distribute the number of slots wihtin different nodes. 

# YCSB-cpp

Yahoo! Cloud Serving
Benchmark([YCSB](https://github.com/brianfrankcooper/YCSB/wiki)) written in C++.
This is a fork of [YCSB-C](https://github.com/basicthinker/YCSB-C) with
following changes.

* Make Zipf distribution and data value more similar to the original YCSB
* Status and latency report during benchmark
* Supported Databases: LevelDB, RocksDB, LMDB

## Building

Simply use `make` to build.

The databases to bind must be specified. You may also need to add additional
link flags (e.g., `-lsnappy`).

To bind LevelDB:

```
make BIND_LEVELDB=1
```

To build with additional libraries and include directories:

```
make BIND_LEVELDB=1 EXTRA_CXXFLAGS=-I/example/leveldb/include \
                    EXTRA_LDFLAGS="-L/example/leveldb/build -lsnappy"
```

Or modify config section in `Makefile`.

RocksDB build example:

```
EXTRA_CXXFLAGS ?= -I/example/rocksdb/include
EXTRA_LDFLAGS ?= -L/example/rocksdb -ldl -lz -lsnappy -lzstd -lbz2 -llz4

BIND_ROCKSDB ?= 1
```

## Running

Load data with leveldb:

```
./ycsb -load -db leveldb -P workloads/workloada -P leveldb/leveldb.properties -s
```

Run workload A with leveldb:

```
./ycsb -run -db leveldb -P workloads/workloada -P leveldb/leveldb.properties -s
```

Load and run workload B with rocksdb:

```
./ycsb -load -run -db rocksdb -P workloads/workloadb -P rocksdb/rocksdb.properties -s
```

Pass additional properties:

```
./ycsb -load -db leveldb -P workloads/workloadb -P rocksdb/rocksdb.properties \
    -p threadcount=4 -p recordcount=10000000 -p leveldb.cache_size=134217728 -s
```
