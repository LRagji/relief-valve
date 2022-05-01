-- To test:
-- redis-cli --eval ./write_count_purge.lue index acc accPurged purge , 10 "*" K1 V1 K2 V2 K3 V3
-- Flowchart: flowcharts/write.drawio
-- Return Codes: Array of strings
-- [0] = Purged == 1 else 0
local indexKey = KEYS[1]
local accKey = KEYS[2]
local accRenameKey = KEYS[3]
local purgeKey = KEYS[4]

local countThreshold = tonumber(ARGV[1])
local dataId = ARGV[2]
local data = ARGV
table.remove(data, 1)
table.remove(data, 1)
local returnArray = {}
local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])

local publishMessageId = redis.call("XADD", accKey, dataId, unpack(data))
table.insert(returnArray, publishMessageId)
redis.call("ZADD", indexKey, currentTimestampInSeconds, accKey)
local currentAccLen = redis.call("XLEN", accKey)
if (currentAccLen >= countThreshold) then
    -- Purge
    redis.call("RENAME", accKey, accRenameKey)
    redis.call("ZREM", indexKey, accKey)
    redis.call("XADD", purgeKey, "*", "Key", accRenameKey)
    table.insert(returnArray, 1)
else
    table.insert(returnArray, 0)
end
return returnArray
