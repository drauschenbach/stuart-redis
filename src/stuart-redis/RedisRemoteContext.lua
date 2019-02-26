local RedisConfig = require 'stuart-redis.RedisConfig'
local RedisContext = require 'stuart-redis.RedisContext'
local stuart = require 'stuart'

local RedisRemoteContext = stuart.class(RedisContext)

function RedisRemoteContext:foreachWithPipeline(redisConf, items, f)
  local conn = redisConf:connection()
  
  -- Pipelines are broke with Lua 5.2; see https://github.com/nrk/redis-lua/issues/43
  --
  -- local replies, count = conn:pipeline(function(pipeline)
  --   for _, item in ipairs(items) do
  --     f(pipeline, item)
  --   end
  -- end)
  
  for _, kv in ipairs(items) do
    f(conn, kv)
  end
  
end

function RedisRemoteContext:setHash(hashName, data, ttl, redisConf)
  self:foreachWithPipeline(redisConf, data, function(pipeline, item)
    local k, v = item[1], item[2]
    pipeline:hset(hashName, k, v)
    if ttl and ttl > 0 then
      pipeline:expire(hashName, ttl)
    end
  end)
end

function RedisRemoteContext:setKVs(data, ttl, redisConf)
  self:foreachWithPipeline(redisConf, data, function(pipeline, item)
    local k, v = item[1], item[2]
    if ttl and ttl > 0 then
      pipeline:setex(k, ttl, v)
    else
      pipeline:set(k, v)
    end
  end)
end

function RedisRemoteContext:setList(listName, data, ttl, redisConf)
  self:foreachWithPipeline(redisConf, data, function(pipeline, v)
    pipeline:rpush(listName, v)
    if ttl and ttl > 0 then
      pipeline:expire(listName, ttl)
    end
  end)
end

function RedisRemoteContext:setSet(setName, data, ttl, redisConf)
  self:foreachWithPipeline(redisConf, data, function(pipeline, v)
    pipeline:sadd(setName, v)
    if ttl and ttl > 0 then
      pipeline:expire(setName, ttl)
    end
  end)
end

function RedisRemoteContext:setZset(zsetName, data, ttl, redisConf)
  self:foreachWithPipeline(redisConf, data, function(pipeline, item)
    local k, v = item[1], tonumber(item[2])
    pipeline:zadd(zsetName, tonumber(v), k)
    if ttl and ttl > 0 then
      pipeline:expire(zsetName, ttl)
    end
  end)
end

function RedisRemoteContext:toRedisHASH(keyValuesRDD, hashName, ttl)
  local redisConf = RedisConfig.newFromSparkConf(self:getConf())
  keyValuesRDD:foreachPartition(function(data)
    self:setHash(hashName, data, ttl, redisConf)
  end)
end

function RedisRemoteContext:toRedisKV(keyValuesRDD, ttl)
  local redisConf = RedisConfig.newFromSparkConf(self:getConf())
  keyValuesRDD:foreachPartition(function(data)
    self:setKVs(data, ttl, redisConf)
  end)
end

function RedisRemoteContext:toRedisLIST(valuesRDD, listName, ttl)
  local redisConf = RedisConfig.newFromSparkConf(self:getConf())
  valuesRDD:foreachPartition(function(data)
    self:setList(listName, data, ttl, redisConf)
  end)
end

function RedisRemoteContext:toRedisSET(valuesRDD, setName, ttl)
  local redisConf = RedisConfig.newFromSparkConf(self:getConf())
  valuesRDD:foreachPartition(function(data)
    self:setSet(setName, data, ttl, redisConf)
  end)
end

function RedisRemoteContext:toRedisZSET(valuesRDD, setName, ttl)
  local redisConf = RedisConfig.newFromSparkConf(self:getConf())
  valuesRDD:foreachPartition(function(data)
    self:setZset(setName, data, ttl, redisConf)
  end)
end

return RedisRemoteContext