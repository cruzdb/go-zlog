# go-zlog - Go bindings for Zlog

Zlog is a strongly consistent, high-performance shared log designed to run on
top of Ceph. More information about Zlog:

* Source: https://github.com/noahdesu/zlog
* System Design: http://noahdesu.github.io/2014/10/26/corfu-on-ceph.html
* Asynchronous API Design: http://noahdesu.github.io/2015/09/04/zlog-async-api.html

[![Build Status](https://travis-ci.org/noahdesu/go-zlog.svg)](https://travis-ci.org/noahdesu/go-zlog) [![Godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/noahdesu/go-zlog)

## Dependencies

Zlog is designed to run on top of the RADOS object store that is part of the
Ceph storage system. Access to RADOS for Go is provided by the go-ceph
project, and is a dependency of go-zlog.

* go-ceph: https://github.com/ceph/go-ceph

## Example

First connect to the RADOS pool that contains (or will contain) the log.

```go
conn, _ := rados.NewConn()
conn.ReadDefaultConfigFile()
conn.Connect()

pool := conn.OpenIOContext("my_log_pool")
```

Next create a brand new log. A given log is striped across objects in a RADOS
pool.  When you create a new log provide a handle to the pool, as well as a
striping width and a name for the log. The host and port information refer to
the network address of the Zlog sequencer. See the zlog project page for more
information about setting up a sequencer.

```go
log := zlog.Create(pool, "mylog", 100, "localhost", "5678")
```

Now the log is available to use. In the following code snippet a string is
appended to the log which returns the position at which the string was stored.
Finally the string at the reported position is read back and verified.

```go
data_in := []byte("My first log entry")
pos := log.Append(data_in)

data_out := make([]byte, len(data_in))
size := log.Read(pos, data_out)

if bytes.Equal(data_in, data_out) == false {
    fmt.Println("Input and output were not equal!")
}
```
