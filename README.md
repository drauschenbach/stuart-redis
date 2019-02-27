<img align="right" src="stuart-redis.png" width="100">

## Stuart-Redis

[![Build Status](https://travis-ci.org/BixData/stuart-redis.svg?branch=master)](https://travis-ci.org/BixData/stuart-redis)
[![License](http://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](LICENSE)
[![Lua](https://img.shields.io/badge/Lua-5.1%20|%205.2%20|%205.3%20|%20JIT%202.0%20|%20JIT%202.1%20-blue.svg)]()

A library for reading and writing data from and to [Redis](https://redis.io) with [Stuart](https://github.com/BixData/stuart), the Apache Spark runtime for embedding and edge computing.

This library can be used in two ways:

* __external__ to a remote Redis server, in which case the [redis-lua](https://luarocks.org/modules/nrk/redis-lua) client library is used to read and write remote data structures
* __embedded__ within an [amalgamated](https://github.com/BixData/lua-amalg-redis) Stuart-based Spark job within a [Redis eval](https://redis.io/commands/eval) command, in which case the `redis` global variable is used to read and write local data structures

Spark data structures are mapped to Redis with guidance from the [spark-redis](https://github.com/RedisLabs/spark-redis) project by Redis Labs, and also mirrors that project's documentation structure.

## Getting Started

```sh
$ luarocks install stuart-redis
```

To work with a remote Redis server, a Redis client libary will also be required. [redis-lua](https://luarocks.org/modules/nrk/redis-lua) is currently chosen because it is based on LuaSocket, which is widely available on many platforms, and [which can also be used within a Go app](https://github.com/BixData/gluasocket).

```sh
$ luarocks install redis-lua
```

## Documentation

* [RDD](./doc/rdd.md)
* [Streaming](./doc/streaming.md)

## Building

```
$ luarocks make stuart-redis-<version>.rockspec
stuart-redis <version> is now installed in /usr/local (license: Apache 2.0)
```

## Testing

```
$ REDIS_URL=redis://localhost:6379 busted -v
```
