package zlog

// #cgo LDFLAGS: -lzlog
// #include <stdlib.h>
// #include <rados/librados.h>
// #include <libzlog.h>
import "C"

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"unsafe"
)

type ZlogError int

func (e ZlogError) Error() string {
	return fmt.Sprintf("zlog: ret=%d", e)
}

type Log struct {
	log C.zlog_log_t
}

func Create(ioctx *rados.IOContext, name string, stripe_size int, host string, port string) (*Log, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	c_host := C.CString(host)
	defer C.free(unsafe.Pointer(c_host))

	c_port := C.CString(port)
	defer C.free(unsafe.Pointer(c_port))

	log := &Log{}

	ret := C.zlog_create(C.rados_ioctx_t(ioctx.Pointer()), c_name, C.int(stripe_size),
		c_host, c_port, &log.log)

	if ret == 0 {
		return log, nil
	} else {
		return nil, ZlogError(int(ret))
	}
}

func Open(ioctx *rados.IOContext, name string, host string, port string) (*Log, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	c_host := C.CString(host)
	defer C.free(unsafe.Pointer(c_host))

	c_port := C.CString(port)
	defer C.free(unsafe.Pointer(c_port))

	log := &Log{}

	ret := C.zlog_open(C.rados_ioctx_t(ioctx.Pointer()), c_name,
		c_host, c_port, &log.log)

	if ret == 0 {
		return log, nil
	} else {
		return nil, ZlogError(int(ret))
	}
}

func OpenOrCreate(ioctx *rados.IOContext, name string, stripe_size int, host string, port string) (*Log, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	c_host := C.CString(host)
	defer C.free(unsafe.Pointer(c_host))

	c_port := C.CString(port)
	defer C.free(unsafe.Pointer(c_port))

	log := &Log{}

	ret := C.zlog_open_or_create(C.rados_ioctx_t(ioctx.Pointer()), c_name, C.int(stripe_size),
		c_host, c_port, &log.log)

	if ret == 0 {
		return log, nil
	} else {
		return nil, ZlogError(int(ret))
	}
}

func (log *Log) Destroy() {
    C.zlog_destroy(C.zlog_log_t(log.log))
}

func (log *Log) CheckTail(next bool) (uint64, error) {
	var c_position C.uint64_t
	var c_next C.int
	if next {
		c_next = 1
	} else {
		c_next = 0
	}
	ret := C.zlog_checktail(C.zlog_log_t(log.log), &c_position, c_next)
	if ret == 0 {
		return uint64(c_position), nil
	} else {
		return 0, ZlogError(int(ret))
	}
}

func (log *Log) Append(data []byte) (uint64, error) {
	var c_position C.uint64_t

	ret := C.zlog_append(C.zlog_log_t(log.log),
		unsafe.Pointer(&data[0]),
		(C.size_t)(len(data)),
		&c_position)

	if ret == 0 {
		return uint64(c_position), nil
	} else {
		return 0, ZlogError(int(ret))
	}
}

func (log *Log) Read(position uint64, data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	ret := C.zlog_read(C.zlog_log_t(log.log), C.uint64_t(position),
		unsafe.Pointer(&data[0]), (C.size_t)(len(data)))

	if ret >= 0 {
		return int(ret), nil
	} else {
		return 0, ZlogError(int(ret))
	}
}

func (log *Log) Fill(position uint64) error {
	ret := C.zlog_fill(C.zlog_log_t(log.log), C.uint64_t(position))
	if ret == 0 {
		return nil
	} else {
		return ZlogError(int(ret))
	}
}
