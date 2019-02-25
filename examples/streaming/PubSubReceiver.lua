local PubSubReceiver = require 'stuart-redis.streaming.PubSubReceiver'
local stuart = require 'stuart'

local sc = stuart.NewContext()
local ssc = stuart.NewStreamingContext(sc, 1.5)

local redisURL = os.getenv('REDIS_URL') or 'redis://127.0.0.1:6379'
local receiver = PubSubReceiver.new(ssc, redisURL, {'mychannel'})
local dstream = ssc:receiverStream(receiver)
dstream:foreachRDD(function(rdd)
  print('Received RDD: ' .. table.concat(rdd:collect(), ','))
end)
ssc:start()
ssc:awaitTermination()
ssc:stop()
