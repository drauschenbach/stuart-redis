local RedisContext = require 'stuart-redis.RedisContext'
local stuart = require 'stuart'

local RedisEmbeddedContext = stuart.class(RedisContext)

function RedisEmbeddedContext:filterKeysByType(keys, typeFilter)
  local res = {}
  for _, key in ipairs(keys) do
    local typeRes = redis.call('TYPE', key)
    local type = typeRes['ok']
    if type == typeFilter then res[#res+1] = key end
  end
  return res
end

function RedisEmbeddedContext:fromRedisHash(keysOrKeyPattern, numPartitions)
  local allKeys = redis.call('KEYS', keysOrKeyPattern)
  local hashKeys = self:filterKeysByType(allKeys, 'hash')
  local res = {}
  for _, hashKey in ipairs(hashKeys) do
    local kvs = redis.call('HGETALL', hashKey)
    for i=2,#kvs,2 do
      local k, v = kvs[i-1], kvs[i]
      res[#res+1] = {k, v}
    end
  end
  return self:parallelize(res, numPartitions)
end

function RedisEmbeddedContext:fromRedisKV(keysOrKeyPattern, numPartitions)
  if type(keysOrKeyPattern) == 'table' then
    error('NIY')
  else
    local allKeys = redis.call('KEYS', keysOrKeyPattern)
    local stringKeys = self:filterKeysByType(allKeys, 'string')
    local mgetRes = redis.call('MGET', unpack(stringKeys))
    local res = {}
    for i, stringKey in ipairs(stringKeys) do
      res[#res+1] = {stringKey, mgetRes[i]}
    end
    return self:parallelize(res, numPartitions)
  end
end

function RedisEmbeddedContext:fromRedisList(keysOrKeyPattern, numPartitions)
  if type(keysOrKeyPattern) == 'table' then
    error('NIY')
  else
    local allKeys = redis.call('KEYS', keysOrKeyPattern)
    local listKeys = self:filterKeysByType(allKeys, 'list')
    local res = {}
    for _, listKey in ipairs(listKeys) do
      local values = redis.call('LRANGE', listKey, 0, -1)
      for _, value in ipairs(values) do
        res[#res+1] = value
      end
    end
    return self:parallelize(res, numPartitions)
  end
end

function RedisEmbeddedContext:fromRedisSet(keysOrKeyPattern, numPartitions)
  if type(keysOrKeyPattern) == 'table' then
    error('NIY')
  else
    local allKeys = redis.call('KEYS', keysOrKeyPattern)
    local setKeys = self:filterKeysByType(allKeys, 'set')
    local res = {}
    for _, setKey in ipairs(setKeys) do
      local values = redis.call('SMEMBERS', setKey)
      for _, value in ipairs(values) do
        res[#res+1] = value
      end
    end
    return self:parallelize(res, numPartitions)
  end
end

function RedisEmbeddedContext:fromRedisZRangeByScore(keysOrKeyPattern, startScore, endScore, numPartitions)
  if type(keysOrKeyPattern) == 'table' then
    error('NIY')
  else
    local allKeys = redis.call('KEYS', keysOrKeyPattern)
    local zsetKeys = self:filterKeysByType(allKeys, 'zset')
    local res = {}
    for _, zsetKey in ipairs(zsetKeys) do
      local values = redis.call('ZRANGEBYSCORE', zsetKey, startScore, endScore)
      for _, value in ipairs(values) do
        res[#res+1] = value
      end
    end
    return self:parallelize(res, numPartitions)
  end
end

function RedisEmbeddedContext:toRedisHash()
  error('NIY')
end

function RedisEmbeddedContext:toRedisKV()
  error('NIY')
end

function RedisEmbeddedContext:toRedisSet()
  error('NIY')
end

function RedisEmbeddedContext:toRedisZset()
  error('NIY')
end

return RedisEmbeddedContext
