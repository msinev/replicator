# replicator
rsync inspired, REDIS optimized, Key-Value based, WAN channels data distribution technology

##status 
Project is a prototype (alpha-beta) version completed but proper code restructure is required.
Reading some topics like https://www.ardanlabs.com/blog/2013/08/organizing-code-to-support-go-get.html
and experimenting on get/build/install functionalities may took some days 

## Task
Data distribution from source to consumers could be quite challenging task. It is usually complicated by QOS requirements, limited bandwith for data, need to support for multiple clients and technologies. 

## Solutions
 There are multiple ways to solve it. Here is a proposed solution  based on key-value storages replication.  Database  replication allways being used as one of the ways to distribute data. Key value storages are quite fiting for such purpose.
This project is focusing on universal key value based replication solution. 
 
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


### Dependencies
```shell script
 go get "github.com/cespare/xxhash"
 go get "github.com/codebear4/ttlcache"
 go get "github.com/cxuhua/lzma"
 go get "github.com/emirpasic/gods/maps/treemap"
 go get "github.com/emirpasic/gods/sets/treeset"
 go get "github.com/golang/protobuf/proto"
 go get "github.com/gorilla/mux"
 go get "github.com/gorilla/websocket"
 go get "github.com/op/go-logging"

```
 


###Redis keys versioning



### Redis replication utility

Programm meant to replicate data one to many REDIS instances. 
Over WAN link with QOS and Compression
```
Usage of REPLICATOR:
  -TLSCA path
        Certificate authority (default "ca.pem")
  -TLSCRT path
        X.509 Certificate (default "certificate.crt,certificate.key")
  -databases id list
        List of databases like 5,0,3,8
  -hub ip:port
        Server address (default ":3333")
  -lv
        Ignore source version - create version chain locally
  -nowait
        Don't wait for client requests termination
  -plaindb id list 
        List of version less databases like 3,8
  -redis ip:port
        Redis address list (default "127.0.0.1:6379")
  -sentinel
        Use sentinel
  -server switch client and server mode
        Listen for requests
  ```  