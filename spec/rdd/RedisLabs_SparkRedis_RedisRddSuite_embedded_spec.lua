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
  file:write('\n')
  file:write("local stuart = require 'stuart'\n")
  file:write("local stuartRedis = require 'stuart-redis'\n")
  file:write('\n')
  file:write('local function split(str, pattern)\n')
  file:write('  local result = {}\n')
  file:write('  for s in string.gmatch(str, pattern) do\n')
  file:write('    table.insert(result, s)\n')
  file:write('  end\n')
  file:write('  return result\n')
  file:write('end\n')
  file:write('\n')
  file:write('local sc = stuart.NewContext()\n')
  file:write('sc = stuartRedis.export(sc)\n')
  file:write('\n')
  file:write("local words = sc:parallelize(split(blogContent, '%w+'))\n")
  file:write("assert(words:count() == 1734, 'expected 1734 words')\n")
  file:write('\n')
  file:write('local wordCounts = words\n')
  file:write('  :map(function(word) return {word, 1} end)\n')
  file:write('  :reduceByKey(function(r, x) return r+x end)\n')
  file:write('  :map(function(e) return {e[1], tostring(e[2])} end)\n')
  file:write('\n')
  file:write(test)
  file:write("return 'SUCCESS'")
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
assert(
  #expectedWordCounts == #actualWordCounts,
  'Expected ' .. #expectedWordCounts .. ' word counts but found ' .. #actualWordCounts)
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
assert(
  #expectedWordCounts == #actualWordCounts,
  'Expected ' .. #expectedWordCounts .. ' word counts but found ' .. #actualWordCounts)
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
assert(
  #expectedWords == #actualWords,
  'Expected ' .. #expectedWords .. ' words but found ' .. #actualWords)
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
assert(
  #expectedWords == #actualWords,
  'Expected ' .. #expectedWords .. ' words but found ' .. #actualWords)
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
assert(
  #expectedWords == #actualWords,
  'Expected ' .. #expectedWords .. ' words but found ' .. #actualWords)
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
assert(
  #expectedWords == #actualWords,
  'Expected ' .. #expectedWords .. ' words but found ' .. #actualWords)
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
  :map(function(x) return {x[1], tostring(#x[2])} end)
  :sortBy(function(x) return x[1] end)
  :collect()
local actualWordCounts = sc:fromRedisZSetWithScore('all:words:cnt:sortedset')
  :sortByKey():collect()
assert(
  #expectedWordCounts == #actualWordCounts,
  'Expected ' .. #expectedWordCounts .. ' word counts but found ' .. #actualWordCounts)
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
