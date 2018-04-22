# replicator
rsync inspired, REDIS optimized, Key-Value based, WAN channels data distribution

## Task
Data distribution from source to consumers could be quite challenging task. It is usually complicated by QOS requirements, limited bandwith for data, need to support for multiple clients and technologies. 

## Solutions
 There are multiple ways to solve it. Here is a proposed solution  based on key-value storages replication.  Database replication allways being used as one of the ways to distribute data. 
 
## Implementation
  * Server golang 
  * Client golang (pushing to REDIS)
  * Client scala (Pushing to CASSANDRA)
  * Client javascript/jquery pushing to array/callback

## Server feature
*  Consumes REDIS (several specified databases) as data source
*  Stream or messaging based media
  *  TCP based replicatio connection 
  *  TCP for initial load + UDP for push updates
  *  HTTP initial load + websocket updates
*  Protobuf based protocol
*  Support key patten subscription/filtration
*  Supports QOS (database/collection based 4 levels)
*  Support data compression
*  Support backpressure flow control
*  Support update merging on slow connections

## Streaming argorithm  
### Server deilvery pipeline per client
 Not including server internal data distribusion pub/sub
 *  Build (filter, merge etc) next update batch (initial or delta) 
 *  Compress 
 *  Chunk (to ~1K size blocks)
 *  Apply QOS to multiple chunk sources
 *  Send to delivery pipe
### Client pipeline 
 * Get block
 * Add to batch
 * Extract and apply as directed by server if batch is completed by current block 
