local RedisContext = require 'stuart-redis.RedisContext'
local RedisEndpoint = require 'stuart-redis.RedisEndpoint'
local SparkConf = require 'stuart.SparkConf'
local stuart = require 'stuart'
local stuartRedis = require 'stuart-redis'

local function generateTestScript(blogContent, test)
  local file, err = io.open('test.lua', 'w')
  assert(err == nil)
  file:write('-- ------------------------------------------------------------\n')
  file:write('-- BEGIN blog content\n')
  file:write('-- ------------------------------------------------------------\n')
  file:write('local blogContent = [[' .. blogContent .. ']]\n')
  file:write('-- ------------------------------------------------------------\n')
  file:write('-- END blog content\n')
  file:write('-- ------------------------------------------------------------\n')
  file:write([[
local moses = require 'moses'
local stuart = require 'stuart'
local stuartRedis = require 'stuart-redis'

local function assertWordsAreSame(expected,actual,message)
  message = message or string.format('Expected %s but got %s', tostring(expected), tostring(actual))
  assert(moses.isEqual(expected, actual), message)
end

local function assertWordCountsAreSame(expected,actual,message)
  message = message or string.format('Expected %d word counts got %d', #expected, #actual)
  assert(#expected == #actual, message)
  for i, expectedKV in ipairs(expected) do
    local expectedWord, expectedCount = expectedKV[1], expectedKV[2]
    local actualKV = actual[i]
    local actualWord, actualCount = actualKV[1], actualKV[2]
    
    local message = string.format(
      'Expected actual word counts at index %d to contain word "%s", but got "%s"',
      i, expectedWord, actualWord)
    assert(expectedWord == actualWord, message)
    
    message = string.format(
      'Expected {%s, %d(%s)} but got {%s, %d(%s)}',
      expectedWord, expectedCount, type(expectedCount),
      actualWord, actualCount, type(actualCount))
    assert(expectedCount == actualCount, message)
    
  end
end

local function split(str, pattern)
  local result = {}
  for s in string.gmatch(str, pattern) do
    table.insert(result, s)
  end
  return result
end

local sc = stuart.NewContext()
sc = stuartRedis.export(sc)

local words = sc:parallelize(split(blogContent, '%w+'))
assert(words:count() == 1734, 'expected 1734 words')

local wordCounts = words
  :map(function(word) return {word, 1} end)
  :reduceByKey(function(r, x) return r+x end)
  :map(function(e) return {e[1], e[2]} end)
]])
  file:write(test)
  file:write("return 'SUCCESS'\n")
  file:close()
  return file
end

local function amalgCapture()
  os.remove('amalg.cache')
  return os.execute('lua -lamalg-redis test.lua')
end

local function amalg()
  return os.execute('amalg-redis.lua'
    .. ' -s test.lua'
    .. ' -o test-with-dependencies.lua'
    .. ' -c'
    .. ' -i "^redis$"' -- omit redis client lib
    .. ' -i "^socket"' -- omit LuaSocket
    .. ' -i "RedisConfig$"' -- omit remote endpoint support
    .. ' -i "RedisEndpoint$"') -- omit remote endpoint support
end

local function replaceAmalgCacheRemoteWithEmbeddedContext()
  local amalgCache = assert(io.open('amalg.cache', 'r'))
  local contents = amalgCache:read('*all')
  contents = string.gsub(contents, 'RedisRemoteContext', 'RedisEmbeddedContext')
  amalgCache:close()
  amalgCache = io.open('amalg.cache', 'w')
  amalgCache:write(contents)
  amalgCache:close()
end

describe('Redis Labs Spark-Redis RedisRddSuite (running embedded within a Redis EVAL)', function()

  local sc, words

  local blog = assert(io.open('spec-fixtures/blog', 'r'))
  local blogContent = blog:read('*all')
  
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
    
    words = sc:parallelize(split(blogContent, '%w+'))
    
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
    generateTestScript(blogContent, [[
local expectedWordCounts = words
  :map(function(word) return {word, 1} end)
  :groupBy(function(e) return e[1] end)
  :map(function(x) return {x[1], tostring(#x[2])} end)
  :sortBy(function(x) return x[1] end)
  :collect()
local redisHashRDD = sc:fromRedisHash('all:words:cnt:hash')
local actualWordCounts = redisHashRDD:sortByKey():collect()
assertWordCountsAreSame(expectedWordCounts, actualWordCounts)
]])
    amalgCapture()
    replaceAmalgCacheRemoteWithEmbeddedContext()
    amalg()
    local testWithDependencies = assert(io.open('test-with-dependencies.lua', 'r'))
    local script = testWithDependencies:read('*all')
    local redisClientLib = require 'redis'
    local redisClient = redisClientLib.connect(os.getenv('REDIS_URL'))
    assert.equals('SUCCESS', redisClient:eval(script, 0))
  end)
  
  
  it('SparkContext:fromRedisKV', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    generateTestScript(blogContent, [[
local expectedWordCounts = words
  :map(function(word) return {word, 1} end)
  :groupBy(function(e) return e[1] end)
  :map(function(x) return {x[1], tostring(#x[2])} end)
  :sortBy(function(x) return x[1] end)
  :collect()
local redisKVRDD = sc:fromRedisKV('*')
local actualWordCounts = redisKVRDD:sortByKey():collect()
assertWordCountsAreSame(expectedWordCounts, actualWordCounts)
]])
    amalgCapture()
    replaceAmalgCacheRemoteWithEmbeddedContext()
    amalg()
    local testWithDependencies = assert(io.open('test-with-dependencies.lua', 'r'))
    local script = testWithDependencies:read('*all')
    local redisClientLib = require 'redis'
    local redisClient = redisClientLib.connect(os.getenv('REDIS_URL'))
    assert.equals('SUCCESS', redisClient:eval(script, 0))
  end)
  
  
  it('SparkContext:fromRedisList', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    generateTestScript(blogContent, [[
local expectedWords = words:sortBy(function(x) return x end):collect()
local redisListRDD = sc:fromRedisList('all:words:list')
local actualWords = redisListRDD:sortBy(function(x) return x end):collect()
assertWordsAreSame(expectedWords, actualWords)
]])
    amalgCapture()
    replaceAmalgCacheRemoteWithEmbeddedContext()
    amalg()
    local testWithDependencies = assert(io.open('test-with-dependencies.lua', 'r'))
    local script = testWithDependencies:read('*all')
    local redisClientLib = require 'redis'
    local redisClient = redisClientLib.connect(os.getenv('REDIS_URL'))
    assert.equals('SUCCESS', redisClient:eval(script, 0))
  end)


  it('SparkContext:fromRedisSet', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    generateTestScript(blogContent, [[
local expectedWords = words:distinct():sortBy(function(x) return x end):collect()
local redisSetRDD = sc:fromRedisSet('all:words:set')
local actualWords = redisSetRDD:sortBy(function(x) return x end):collect()
assertWordsAreSame(expectedWords, actualWords)
]])
    amalgCapture()
    replaceAmalgCacheRemoteWithEmbeddedContext()
    amalg()
    local testWithDependencies = assert(io.open('test-with-dependencies.lua', 'r'))
    local script = testWithDependencies:read('*all')
    local redisClientLib = require 'redis'
    local redisClient = redisClientLib.connect(os.getenv('REDIS_URL'))
    assert.equals('SUCCESS', redisClient:eval(script, 0))
  end)
  
  
  -- TODO fromRedisZRange
  
  
  it('SparkContext:fromRedisZRangeByScore', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    generateTestScript(blogContent, [[
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
assertWordsAreSame(expectedWords, actualWords)
]])
    amalgCapture()
    replaceAmalgCacheRemoteWithEmbeddedContext()
    amalg()
    local testWithDependencies = assert(io.open('test-with-dependencies.lua', 'r'))
    local script = testWithDependencies:read('*all')
    local redisClientLib = require 'redis'
    local redisClient = redisClientLib.connect(os.getenv('REDIS_URL'))
    assert.equals('SUCCESS', redisClient:eval(script, 0))
  end)
  
  
  -- TODO fromRedisZRangeByScoreWithScore
  
  
  -- TODO fromRedisZRangeWithScore
  
  
  it('SparkContext:fromRedisZSet', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    generateTestScript(blogContent, [[
local expectedWords = words:sortBy(function(x) return x end):distinct():collect()
local actualWords = sc:fromRedisZSet('all:words:cnt:sortedset'):sortBy(function(x) return x end):collect()
assertWordsAreSame(expectedWords, actualWords)
]])
    amalgCapture()
    replaceAmalgCacheRemoteWithEmbeddedContext()
    amalg()
    local testWithDependencies = assert(io.open('test-with-dependencies.lua', 'r'))
    local script = testWithDependencies:read('*all')
    local redisClientLib = require 'redis'
    local redisClient = redisClientLib.connect(os.getenv('REDIS_URL'))
    assert.equals('SUCCESS', redisClient:eval(script, 0))
  end)


  it('SparkContext:fromRedisZSetWithScore', function()
    if not stuart.istype(sc, RedisContext) then return pending('No REDIS_URL is configured') end
    generateTestScript(blogContent, [[
local expectedWordCounts = words
  :map(function(word) return {word, 1} end)
  :groupBy(function(x) return x[1] end)
  :map(function(x) return {x[1], #x[2]} end)
  :sortBy(function(x) return x[1] end)
  :collect()
local actualWordCounts = sc:fromRedisZSetWithScore('all:words:cnt:sortedset')
  :sortByKey():collect()
assertWordCountsAreSame(expectedWordCounts, actualWordCounts)
]])
    amalgCapture()
    replaceAmalgCacheRemoteWithEmbeddedContext()
    amalg()
    local testWithDependencies = assert(io.open('test-with-dependencies.lua', 'r'))
    local script = testWithDependencies:read('*all')
    local redisClientLib = require 'redis'
    local redisClient = redisClientLib.connect(os.getenv('REDIS_URL'))
    assert.equals('SUCCESS', redisClient:eval(script, 0))
  end)
  
  
end)
