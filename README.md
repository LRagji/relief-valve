# Relief Valve
This is a simple library for Redis Streams data type, which is used to accumulate messages until a specified threshold is reached, post which the same is available to consumer stream.

To honor time threshold it is mandated to keep atleast one instance of the library running and connected to redis, as there are not redis server side time triggers available.

This package also handles message routing/distribution for advanced scenarios as sated in the example below.

This package can be used in following topologies
1. Simple publish subscribe(poll) sharded que with message acknowledge & lost message reprocess facility
2. Simple publish subscribe(poll) fan-out que with message acknowledge & lost message reprocess facility
3. Batching/Accumalator(by time and element count) sharded que with message acknowledge & lost message reprocess facility
4. Batching/Accumalator(by time and element count) fan-out que with message acknowledge & lost message reprocess facility

Usage Pattern:
1. The package should be instantiated on publisher and subscriber side with identitcal parameters in constructor apart from groupName and clientName, else will lead to chaotic behaviour of pulling messages.
2. Count threshold is always evlauted on writes/publish into stream.
3. Time threshold needs external invocation as redis currently doesnot support cron jobs, either subscriber or publisher can invoke this validation.
4. Highest accurary of time threshold is limited to one second, but depends heavily on external invocation frequency.    

