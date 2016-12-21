BTrDB golang bindings
=====================

[![GoDoc](https://godoc.org/gopkg.in/btrdb.v3?status.svg)](http://godoc.org/gopkg.in/btrdb.v3)

These are the go BTrDB bindings. This branch is compatible with version 3, the most widely deployed version of BTrDB.

You can read the API documentation and code examples by clicking the godoc button above. To import this package in your code, add

```
import "gopkg.in/btrdb.v3"
```

roadmap:

- implement lookup function
 + add touch option that will create stream if it does not exist
- implement list collections function
- implement fork function
- get database serving all the inserts and queries
- get the client bindings serving all inserts and queries, to a single endpoint
- get the database to refuse writes it doesn't hold the lock for
- get the database to flush write buffers on mash change
- get the database to out itself on ctrl-c
- create a layer around the client bindings that routes requests to the correct endpoint with context timeout and failover
