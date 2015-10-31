package zlog

// #cgo LDFLAGS: -lzlog
// #include <stdlib.h>
// #include <rados/librados.h>
// #include <libzlog.h>
import "C"

import (
	"unsafe"
)

type Stream struct {
    log C.zlog_log_t
    stream C.zlog_stream_t
}

func (log *Log) OpenStream(id uint64) (*Stream, error) {
    stream := &Stream{}

    ret := C.zlog_stream_open(C.zlog_log_t(log.log), C.uint64_t(id),
        &stream.stream)

    if ret == 0 {
        return stream, nil
    } else {
        return nil, ZlogError(int(ret))
    }
}

func (log *Log) MultiAppend(data []byte, stream_ids []uint64) (uint64, error) {
    var c_position C.uint64_t
    var c_stream_ids *C.uint64_t = nil
    if len(stream_ids) > 0 {
        c_stream_ids = (*C.uint64_t)(unsafe.Pointer(&stream_ids[0]))
    }
    ret := C.zlog_multiappend(C.zlog_log_t(log.log),
        unsafe.Pointer(&data[0]),
        (C.size_t)(len(data)),
        c_stream_ids,
        (C.size_t)(len(stream_ids)),
        &c_position)
    if ret == 0 {
        return uint64(c_position), nil
    } else {
        return 0, ZlogError(int(ret))
    }
}

func (stream *Stream) Append(data []byte) (uint64, error) {
    var c_position C.uint64_t
    ret := C.zlog_stream_append(C.zlog_stream_t(stream.stream),
        unsafe.Pointer(&data[0]),
        (C.size_t)(len(data)),
        &c_position)
    if ret == 0 {
        return uint64(c_position), nil
    } else {
        return 0, ZlogError(int(ret))
    }
}

func (stream *Stream) ReadNext(data []byte) (int, uint64, error) {
    var c_position C.uint64_t
    ret := C.zlog_stream_readnext(C.zlog_stream_t(stream.stream),
        unsafe.Pointer(&data[0]),
        (C.size_t)(len(data)),
        &c_position)
    if ret >= 0 {
        return int(ret), uint64(c_position), nil
    } else {
        return 0, 0, ZlogError(int(ret))
    }
}

func (stream *Stream) Reset() error {
    ret := C.zlog_stream_reset(C.zlog_stream_t(stream.stream))
    if ret == 0 {
        return nil
    } else {
        return ZlogError(int(ret))
    }
}

func (stream *Stream) Sync() error {
    ret := C.zlog_stream_sync(C.zlog_stream_t(stream.stream))
    if ret == 0 {
        return nil
    } else {
        return ZlogError(int(ret))
    }
}

func (stream *Stream) Id() uint64 {
    ret := C.zlog_stream_id(C.zlog_stream_t(stream.stream))
    return uint64(ret)
}

func (stream *Stream) History() []uint64 {
    for {
        ret := C.zlog_stream_history(C.zlog_stream_t(stream.stream), nil, 0)
        pos := make([]uint64, ret)
        ret = C.zlog_stream_history(C.zlog_stream_t(stream.stream),
            (*C.uint64_t)(&pos[0]), (C.size_t)(len(pos)))
        if len(pos) == int(ret) {
            return pos
        }
    }
}

func (log *Log) StreamMembership(position uint64) ([]uint64, error) {
    stream_ids := make([]uint64, 1)
    for {
        ret := C.zlog_stream_membership(C.zlog_log_t(log.log),
            (*C.uint64_t)(&stream_ids[0]), (C.size_t)(len(stream_ids)),
            C.uint64_t(position))
        if ret < 0 {
            return nil, ZlogError(int(ret))
        } else if int(ret) <= len(stream_ids) {
            return stream_ids[:ret], nil
        } else {
            stream_ids = make([]uint64, ret)
        }
    }
}
