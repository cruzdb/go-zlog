#!/bin/bash
set -e
set -x

# startup a ceph cluster
mkdir /tmp/ceph
bash /src/micro-osd.sh /tmp/ceph

ZLOG_BRANCH=${zlog_branch:-master}
GOZLOG_BRANCH=${gozlog_branch:-master}

apt-get install -y libprotobuf-dev protobuf-compiler golang

# setup golang
mkdir -p /src/go
export GOPATH=/src/go
export PATH=/src/go/bin:$PATH

# install zlog. this is done in the entry point so it will
# pull the latest zlog source each time the container is run
cd /src

ls -l

if [ ! -d /src/zlog ]; then
  git clone --branch=$ZLOG_BRANCH https://github.com/noahdesu/zlog.git
fi

cd zlog
git status

autoreconf -ivf
./configure --prefix=/usr
make
make install

cd /src/zlog/src
export CEPH_CONF=/tmp/ceph/ceph.conf
./zlog-seqr --port 5678 --daemon

go get github.com/ceph/go-ceph/rados
go get github.com/stretchr/testify/assert

if [ ! -d $GOPATH/src/github.com/noahdesu/go-zlog ]; then
  mkdir -p $GOPATH/src/github.com/noahdesu
  cd $GOPATH/src/github.com/noahdesu
  git clone --branch=$GOZLOG_BRANCH https://github.com/noahdesu/go-zlog.git
fi

cd $GOPATH/src/github.com/noahdesu/go-zlog
go build
go install

git status

go test -v ./...
