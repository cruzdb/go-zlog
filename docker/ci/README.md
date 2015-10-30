go-zlog-ci
==========

This Dockerfile is used to create an image runs the continuous integration
test suite for the Go bindings to zlog.

Usage
-----

By default the image will checkout and build go-zlog from the master branch of
the repository located at https://github.com/noahdesu/go-zlog:

  `docker run zlog/go-ci`

Use `-e "gozlog_branch=different-branch` to checkout a different branch:

  `docker run -e "gozlog_branch=testing" zlog/go-ci`

A go-zlog source directory on the host can be used instead using a bind mount.
In this case the `gozlog_branch` environment variable is ignored:

  `docker run -v /tmp/asdf:/src/go/src/github.com/noahdesu/go-zlog zlog/go-ci`

In each case the master branch of zlog is used. The `zlog_branch` environment
variable can be used to specify a specific branch of zlog to test against.
