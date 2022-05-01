-- To test:
-- redis-cli --eval ./time_purge.lua index acc accPurged purge , 10
-- Flowchart: flowcharts/write.drawio
-- Return Codes: Array of strings
-- [0] = Purged == 1 else 0
local indexKey = KEYS[1]
local accKey = KEYS[2]
local accRenameKey = KEYS[3]
local purgeKey = KEYS[4]

local timeThreshold = tonumber(ARGV[1])
local returnArray = {}
local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])

local lastActivityTime = redis.call("ZSCORE", indexKey, accKey)
if (type(lastActivityTime) == "string" and (currentTimestampInSeconds - tonumber(lastActivityTime)) >= timeThreshold) then
    -- Purge
    redis.call("RENAME", accKey, accRenameKey)
    redis.call("ZREM", indexKey, accKey)
    redis.call("XADD", purgeKey, "*", "Key", accRenameKey)
    table.insert(returnArray, 1)
else
    table.insert(returnArray, 0)
end
return returnArray
