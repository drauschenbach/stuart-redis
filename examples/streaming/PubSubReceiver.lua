local PubSubReceiver = require 'stuart-redis.streaming.PubSubReceiver'
local RedisEndpoint = require 'stuart-redis.RedisEndpoint'
local SparkConf = require 'stuart.SparkConf'
local stuart = require 'stuart'

local redisUrl = os.getenv('REDIS_URL')
assert(redisUrl)
local redisEndpoint = RedisEndpoint.newFromURI(redisUrl)
local conf = SparkConf.new()
  :setMaster('local[1]')
  :setAppName('Stuart-Redis PubSubReceiver Example')
  :set('spark.redis.host'   , redisEndpoint.host)
  :set('spark.redis.port'   , redisEndpoint.port)
  :set('spark.redis.db'     , redisEndpoint.dbNum)
  :set('spark.redis.timeout', redisEndpoint.timeout)
if redisEndpoint.auth then conf:set('spark.redis.auth', redisEndpoint.auth) end

local sc = stuart.NewContext(conf)
local ssc = stuart.NewStreamingContext(sc, 1.5)

local receiver = PubSubReceiver.new(ssc, {'mychannel'})
local dstream = ssc:receiverStream(receiver)
dstream:foreachRDD(function(rdd)
  print('Received RDD: ' .. table.concat(rdd:collect(), ','))
end)
ssc:start()
ssc:awaitTermination()
ssc:stop()
