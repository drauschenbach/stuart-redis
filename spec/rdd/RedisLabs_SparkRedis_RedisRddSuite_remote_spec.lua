local RedisContext = require 'stuart-redis.RedisContext'
local RedisEndpoint = require 'stuart-redis.RedisEndpoint'
local SparkConf = require 'stuart.SparkConf'
local stuart = require 'stuart'
local stuartRedis = require 'stuart-redis'

describe('Redis Labs Spark-Redis RedisRddSuite (using a remote Redis server)', function()

  local sc, words

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

    words = sc:parallelize(split(content, '%w+'))
    
    local wordCounts = words
      :map(function(word) return {word, 1} end)
      :reduceByKey(function(r, x) return r+x end)
      :map(function(e) return {e[1], tostring(e[2])} end)
    
    sc:toRedisKV  (wordCounts)
    sc:toRedisZSET(wordCounts, 'all:words:cnt:sortedset')
    sc:toRedisHASH(wordCounts, 'all:words:cnt:hash')
    sc:toRedisLIST(words     , 'all:words:list')
    sc:toRedisSET (words     , 'all:words:set')
  end)

  it('SparkContext:fromRedisHash', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWordCounts = words
      :map(function(word) return {word, 1} end)
      :groupBy(function(e) return e[1] end)
      :map(function(x) return {x[1], tostring(#x[2])} end)
      :sortBy(function(x) return x[1] end)
      :collect()
    local redisHashRDD = sc:fromRedisHash('all:words:cnt:hash')
    local actualWordCounts = redisHashRDD:sortByKey():collect()
    assert.same(expectedWordCounts, actualWordCounts)
  end)
  
  it('SparkContext:fromRedisKV', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWordCounts = words
      :map(function(word) return {word, 1} end)
      :groupBy(function(e) return e[1] end)
      :map(function(x) return {x[1], tostring(#x[2])} end)
      :sortBy(function(x) return x[1] end)
      :collect()
    local redisKVRDD = sc:fromRedisKV('*')
    local actualWordCounts = redisKVRDD:sortByKey():collect()
    assert.same(expectedWordCounts, actualWordCounts)
  end)
  
  it('SparkContext:fromRedisList', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWords = words:sortBy(function(x) return x end):collect()
    local redisListRDD = sc:fromRedisList('all:words:list')
    local actualWords = redisListRDD:sortBy(function(x) return x end):collect()
    assert.same(expectedWords, actualWords)
  end)
  
  it('SparkContext:fromRedisSet', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWords = words:distinct():sortBy(function(x) return x end):collect()
    local redisSetRDD = sc:fromRedisSet('all:words:set')
    local actualWords = redisSetRDD:sortBy(function(x) return x end):collect()
    assert.same(expectedWords, actualWords)
  end)
  
  -- disable until Stuart 2.0.3 ships due to Stuart issue-129
  --[[
  it('SparkContext:fromRedisZRange', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWordCounts = words
      :map(function(word) return {word, 1} end)
      :groupBy(function(x) return x[1] end)
      :map(function(x) return {x[1], tostring(#x[2])} end)
      :sortBy(function(x) return x[1] end)
    local expectedWords = expectedWordCounts
      :sortBy(function(x) return {x[2], x[1]} end)
      :map(function(x) return x[1] end)
      :take(16)
    local actualWords = sc:fromRedisZRange('all:words:cnt:sortedset', 0, 15):collect()
    assert.same(expectedWords, actualWords)
  end)
  --]]
  
  it('SparkContext:fromRedisZRangeByScore', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWordCounts = words
      :map(function(word) return {word, 1} end)
      :groupBy(function(x) return x[1] end)
      :map(function(x) return {x[1], #x[2]} end)
      :filter(function(x) return x[2] >= 3 and x[2] <= 9 end)
    local expectedWords = expectedWordCounts
      :map(function(x) return x[1] end)
      :sortBy(function(x) return x end)
      :collect()
    local actualWords = sc:fromRedisZRangeByScore('all:words:cnt:sortedset', 3, 9)
      :sortBy(function(x) return x end):collect()
    assert.same(expectedWords, actualWords)
  end)

  -- disable until Stuart 2.0.3 ships due to Stuart issue-129
  --[[
  it('SparkContext:fromRedisZRangeByScoreWithScore', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWordCounts = words
      :map(function(word) return {word, 1} end)
      :groupBy(function(x) return x[1] end)
      :map(function(x) return {x[1], #x[2]} end)
      :filter(function(x) return x[2] >= 3 and x[2] <= 9 end)
      :sortBy(function(x) return {x[2], x[1]} end)
      :map(function(x) return {x[1], tostring(x[2])} end)
      :collect()
    local actualWordCounts = sc:fromRedisZRangeByScoreWithScore('all:words:cnt:sortedset', 3, 9)
      :sortBy(function(x) return {x[2], x[1]} end)
      :collect()
    assert.same(expectedWordCounts, actualWordCounts)
  end)
  --]]

  -- disable until Stuart 2.0.3 ships due to Stuart issue-129
  --[[
  it('SparkContext:fromRedisZRangeWithScore', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWordCounts = words
      :map(function(word) return {word, 1} end)
      :groupBy(function(x) return x[1] end)
      :map(function(x) return {x[1], tostring(#x[2])} end)
      :sortBy(function(x) return {x[2], x[1]} end)
      :take(16)
    local actualWordCounts = sc:fromRedisZRangeWithScore('all:words:cnt:sortedset', 0, 15):collect()
    assert.same(expectedWordCounts, actualWordCounts)
  end)
  --]]
  
  it('SparkContext:fromRedisZSet', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWords = words:sortBy(function(x) return x end):distinct():collect()
    local actualWords = sc:fromRedisZSet('all:words:cnt:sortedset'):sortBy(function(x) return x end):collect()
    assert.same(expectedWords, actualWords)
  end)
  
  it('SparkContext:fromRedisZSetWithScore', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    local expectedWordCounts = words
      :map(function(word) return {word, 1} end)
      :groupBy(function(x) return x[1] end)
      :map(function(x) return {x[1], #x[2]} end)
      :sortBy(function(x) return x[1] end)
      :collect()
    local actualWordCounts = sc:fromRedisZSetWithScore('all:words:cnt:sortedset')
      :sortByKey():collect()
    assert.same(expectedWordCounts, actualWordCounts)
  end)
  
end)
