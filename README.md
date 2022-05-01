# Relief Valve
This is a simple library for Redis Streams data type, which is used to accumulate messages until a specified threshold is reached, post which the same is available to consumer stream.

To keep memory i/o low as possible consumers on the main stream get reference keys to an accumulated message rather than the content of the message itself.
 
