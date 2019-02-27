local aristotleSaid = [[We are what we repeatedly do. Excellence,
then, is not an act, but a habit. It is the mark of an educated
mind to be able to entertain a thought without accepting it.]]

local stuart = require 'stuart'
local stuartRedis = require 'stuart-redis'

local function splitUsingGlob(str, pattern)
  local result = {}
  for s in string.gmatch(str, pattern) do
    table.insert(result, s)
  end
  return result
end

local sc = stuart.NewContext()
sc = stuartRedis.export(sc)
    
local words = sc:parallelize(splitUsingGlob(aristotleSaid, '%w+'))
    
local wordCounts = words
  :map(function(word) return {word, 1} end)
  :reduceByKey(function(r, x) return r+x end)
  :map(function(e) return {e[1], tostring(e[2])} end)
    
sc:toRedisZSET(wordCounts, 'aristotleWordCounts')
