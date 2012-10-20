# General

ClusterMate project implements basic components of a multi-node storage system
using single-node storage components provided by
[StoreMate](https://github.com/cowtowncoder/StoreMate) project.

Check out [Project Wiki](ClusterMate/wiki) for complete description.

# Status

Project getting ready for its first release -- mostly working, unit tested.

Stay tuned!

# Features

Default storage system has following features:

* Data store
 * Immutable: that is, write-once, can not modify
 * No size limitations, storage works for data of any size, although may not be optimal for smallest payloads
 * Metadata/data separation: metadata stored in a database (by StoreMate), data on filesystem or inlined (small entries)
 * Automatic on-the-fly compression of data (not metadata)
* Access
 * Clustered CRUD (PUT, GET/HEAD, DELETE)
 * Streaming (for large data)
 * On-the-fly compression/uncompression
 * Hash-based routing
 * Range queries within specified prefixes (that is, routing can use path prefixes)
* Distributed operation
 * Configurable N-way replication and sharding
 * Automatic server-to-server data repair to handle partial updates and transient outages
 * Clients can choose subset of N for successful operations with lower latency

# Sub-modules

Project is a multi-module Maven project.
Sub-modules are can be grouped in following categories:

* api: Basic datatypes shared by client and service modules
* `json`: [Jackson](https://github.com/FasterXML/jackson-databind) converters for core datatypes from 'api' module and StoreMate
or client-server and server-server communication
* client:
 * single-node access components ("call")
 * clustered-access ("operation")
* service:


