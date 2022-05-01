# Relief Valve
This is a simple library for Redis Streams data type, which is used to accumulate messages until a specified threshold is reached, post which the same is available to consumer stream.

To honor time threshold it is mandated to keep atleast one instance of the library running and connected to redis, as there are not redis server side time triggers available.

This package also handles message routing/distribution for advanced scenarios as sated in the example below.

This package handles the read unique message per client topology of delivering messages not fan-out.

