<img align="right" src="stuart.png" width="70">

## Extended Redis support for Stuart

[Stuart](https://github.com/BixData/stuart) is the Apache Spark runtime for embedding and edge computing.

[Redis](https://redis.io) is your favorite "app server" and data structure service.

This project contains extensions to Stuart that are specific to working with Redis.

[![License](http://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](LICENSE)

## Getting Started

To use these examples on an operating system, start by installing a Redis client libary. My current preference is one based on LuaSocket, [because I can also use it in Go apps](https://github.com/BixData/gluasocket):

```sh
$ luarocks install stuart
$ luarocks install redis-lua
```

## Spark Streaming Pub/Sub Receiver

If you have a Spark Streaming based loop that would like to ingest data from a Redis Pub/Sub channel, use the `PubSubReceiver` class:

```lua
local stuart = require 'stuart'
local PubSubReceiver = require './PubSubReceiver'

local sc = stuart.NewContext()
local ssc = stuart.NewStreamingContext(sc, 1.5)

local redisUrl = os.getenv('REDIS_URL') or 'redis://127.0.0.1:6379'
local receiver = PubSubReceiver.new(ssc, redisUrl, {'mychannel'})
local dstream = ssc:receiverStream(receiver)
dstream:foreachRDD(function(rdd)
  print('Received RDD: ' .. table.concat(rdd:collect(), ','))
end)
ssc:start()
ssc:awaitTermination()
ssc:stop()
```

Run the sample Spark Streaming loop:

```sh
$ cd pubsub
$ REDIS_URL=redis://127.0.0.1:6379/mychannel lua main.lua
```

And then feed its ingest engine by publishing to Redis. The default batch size set in `main.lu` is 1.5 seconds, which gives you enough time to publish two messages into the same RDD:

```sh
$ redis-cli
127.0.0.1:6379> publish mychannel one
127.0.0.1:6379> publish mychannel two
127.0.0.1:6379> publish mychannel three
```

The Spark Streaming loop will report:

```	
$ lua main.lua 
INFO Running Stuart (Embedded Spark 2.2.0)
INFO Connected to redis://127.0.0.1:6379
INFO Subscribed to channel mychannel
Received RDD: one
Received RDD: two,three
```

The full example is in the [pubsub/](./pubsub/) folder.
