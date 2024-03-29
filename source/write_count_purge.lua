-- To test:
-- redis-cli --eval ./write_count_purge.lua index acc purge , 10 "*" K1 V1 K2 V2 K3 V3
-- Flowchart: flowcharts/write.drawio
-- Return Codes: Array of strings
-- [0] = Purged == 1 else 0
local indexKey = KEYS[1]
local accKey = KEYS[2]
local purgeKey = KEYS[3]

local countThreshold = tonumber(ARGV[1])
local dataId = ARGV[2]
local idPropName = ARGV[3]
local maxlength = tonumber(ARGV[4])
local releaseCount = tonumber(ARGV[5])
local data = ARGV
table.remove(data, 1)
table.remove(data, 1)
table.remove(data, 1)
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
        if (maxlength >= 1) then
            redis.call("XADD", purgeKey, "MAXLEN", "~", maxlength, "*", unpack(clonedData))
        else
            redis.call("XADD", purgeKey, "*", unpack(clonedData))
        end
    end
    redis.call("XDEL", accKey, unpack(streamIDsToDrop))
    table.insert(returnArray, 1)
else
    table.insert(returnArray, 0)
end

return returnArray
