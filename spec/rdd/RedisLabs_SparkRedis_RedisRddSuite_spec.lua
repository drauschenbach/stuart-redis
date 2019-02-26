local RedisEndpoint = require 'stuart-redis.RedisEndpoint'
local SparkConf = require 'stuart.SparkConf'
local stuart = require 'stuart'
local stuartRedis = require 'stuart-redis'

describe('Redis Labs Spark-Redis RedisRddSuite', function()

  local sc

  local blog = assert(io.open('spec-fixtures/blog', 'r'))
  local content = blog:read('*all')
  
  local function split(str, pattern)
    local result = {}
    for s in string.gmatch(str, pattern) do
      table.insert(result, s)
    end
    return result
  end

  setup(function()
    local redisUrl = os.getenv('REDIS_URL')
    local redisClientLib = require 'redis'
    if redisUrl == nil or redisClientLib == nil then
      return
    end
    local redisClient = redisClientLib.connect(redisUrl)
    redisClient:flushdb()
    
    local redisEndpoint = RedisEndpoint.newFromURI(redisUrl)
    local conf = SparkConf.new()
    conf:setMaster('local[1]')
      :setAppName('RedisLabs::Spark-Redis::RedisRddSuite')
      :set('spark.redis.host'   , redisEndpoint.host)
      :set('spark.redis.port'   , redisEndpoint.port)
      :set('spark.redis.db'     , redisEndpoint.dbNum)
      :set('spark.redis.timeout', redisEndpoint.timeout)
    if redisEndpoint.auth then conf:set('spark.redis.auth', redisEndpoint.auth) end
    
    sc = stuart.NewContext(conf)
    sc = stuartRedis.export(sc)

    local words = sc:parallelize(split(content, '%w+'))
    
    local wordCounts = words
      :map(function(word) return {word, 1} end)
      :reduceByKey(function(r, x) return r+x end)
      :map(function(e) return {e[1], tostring(e[2])} end)
    
    sc:toRedisKV(wordCounts)
    sc:toRedisZSET(wordCounts, 'all:words:cnt:sortedset')
    sc:toRedisHASH(wordCounts, 'all:words:cnt:hash')
    sc:toRedisLIST(words, 'all:words:list')
    sc:toRedisSET(words, 'all:words:set')
  end)

  it('TODO...', function()
    local redisUrl = os.getenv('REDIS_URL')
    if not redisUrl then return pending('No REDIS_URL is configured') end
    -- TODO
  end)

end)