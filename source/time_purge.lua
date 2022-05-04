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
local returnArray = {}

local actualScore = redis.call("ZSCORE", indexKey, accKey)
if (actualScore == score) then -- This check is done to make actual operations transactional and thread safe.
    -- Purge
    local clonedData = {}
    local results = redis.call("XRANGE", accKey, "-", "+")
    for _, data in ipairs(results) do
        table.insert(clonedData, idPropName)
        table.insert(clonedData, data[1])
        for _, kvp in ipairs(data[2]) do
            table.insert(clonedData, kvp)
        end
    end
    -- Insert
    if (#clonedData > 0) then
        redis.call("XADD", purgeKey, "*", unpack(clonedData))
    end
    redis.call("DEL", accKey)
    redis.call("ZREM", indexKey, accKey)
    table.insert(returnArray, 1)
else
    table.insert(returnArray, 0)
end

return returnArray
