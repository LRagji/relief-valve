-- To test:
-- redis-cli --eval ./time_purge.lua index acc purge , 10
-- Flowchart: flowcharts/write.drawio
-- Return Codes: Array of strings
-- [0] = Purged == 1 else 0
local indexKey = KEYS[1]
local accKey = KEYS[2]
local purgeKey = KEYS[3]

local score = ARGV[1]
local idPropName = ARGV[2]
local releaseCount = tonumber(ARGV[3])
local refreshTimeOnSucessfullPurge = tonumber(ARGV[4])
local returnArray = {}

local actualScore = redis.call("ZSCORE", indexKey, accKey)
if (actualScore == score) then -- This check is done to make actual operations transactional and thread safe.
    -- Purge
    local clonedData = {}
    local results
    if (releaseCount == -1) then
        results = redis.call("XRANGE", accKey, "-", "+")
    else
        results = redis.call("XRANGE", accKey, "-", "+", "COUNT", releaseCount)
    end
    local streamIDsToDrop = {}
    for _, data in ipairs(results) do
        local streamId = data[1]
        table.insert(clonedData, idPropName)
        table.insert(clonedData, streamId)
        for _, kvp in ipairs(data[2]) do
            table.insert(clonedData, kvp)
        end
        table.insert(streamIDsToDrop, streamId)
    end
    -- Insert
    if (#clonedData > 0) then
        redis.call("XADD", purgeKey, "*", unpack(clonedData))
    end
    redis.call("XDEL", accKey, unpack(streamIDsToDrop))
    if (refreshTimeOnSucessfullPurge == 0) then
        local tempTime = redis.call("TIME")
        local currentTimestampInSeconds = tonumber(tempTime[1])
        redis.call("ZADD", indexKey, currentTimestampInSeconds, accKey)
    end
    table.insert(returnArray, 1)
else
    table.insert(returnArray, 0)
end

return returnArray
