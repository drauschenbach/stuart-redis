local RedisContext = require 'stuart-redis.RedisContext'
local stuart = require 'stuart'

local RedisEmbeddedContext = stuart.class(RedisContext)

function RedisEmbeddedContext:filterKeysByType(keys, typeFilter)
  local res = {}
  for _, key in ipairs(keys) do
    local typeRes = redis.call('TYPE', key)
    for k,v in pairs(typeRes) do
    end
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
