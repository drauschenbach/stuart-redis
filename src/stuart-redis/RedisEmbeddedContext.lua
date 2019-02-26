local RedisContext = require 'stuart-redis.RedisContext'
local stuart = require 'stuart'

local RedisEmbeddedContext = stuart.class(RedisContext)

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
