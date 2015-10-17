package zlog_test

import "testing"
import "github.com/ceph/go-ceph/rados"
import "github.com/stretchr/testify/assert"
import "github.com/noahdesu/go-zlog/zlog"
import "os/exec"

func GetUUID() string {
	out, _ := exec.Command("uuidgen").Output()
	return string(out[:36])
}

func TestCreate(t *testing.T) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	conn.Connect()

	poolname := GetUUID()
	err := conn.MakePool(poolname)
	assert.NoError(t, err)

	pool, err := conn.OpenIOContext(poolname)
	assert.NoError(t, err)

    _, err = zlog.Create(pool, "mylog", 0, "localhost", "5678")
    assert.Error(t, err, "Invalid")

    _, err = zlog.Create(pool, "mylog", -1, "localhost", "5678")
    assert.Error(t, err, "Invalid")

    _, err = zlog.Create(pool, "", 5, "localhost", "5678")
    assert.Error(t, err, "Invalid")

    log, err := zlog.Create(pool, "mylog", 5, "localhost", "5678")
    assert.NoError(t, err)

    log.Destroy()

    _, err = zlog.Create(pool, "mylog", 5, "localhost", "5678")
    assert.Error(t, err, "Exists")

    pool.Destroy()
    conn.Shutdown()
}

func TestOpen(t *testing.T) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	conn.Connect()

	poolname := GetUUID()
	err := conn.MakePool(poolname)
	assert.NoError(t, err)

	pool, err := conn.OpenIOContext(poolname)
	assert.NoError(t, err)

    _, err = zlog.Open(pool, "", "localhost", "5678")
    assert.Error(t, err, "Invalid")

    _, err = zlog.Open(pool, "dne", "localhost", "5678")
    assert.Error(t, err, "Doesn't exist")

    log, err := zlog.Create(pool, "mylog", 5, "localhost", "5678")
    assert.NoError(t, err)
    log.Destroy()

    log, err = zlog.Open(pool, "mylog", "localhost", "5678")
    assert.NoError(t, err)
    log.Destroy()

    pool.Destroy()
    conn.Shutdown()
}

func TestCheckTail(t *testing.T) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	conn.Connect()

	poolname := GetUUID()
	err := conn.MakePool(poolname)
	assert.NoError(t, err)

	pool, err := conn.OpenIOContext(poolname)
	assert.NoError(t, err)

    log, err := zlog.Create(pool, "mylog", 5, "localhost", "5678")
    assert.NoError(t, err)

    pos, err := log.CheckTail(false)
    assert.NoError(t, err)
    assert.Equal(t, pos, uint64(0))

    pos, err = log.CheckTail(false)
    assert.NoError(t, err)
    assert.Equal(t, pos, uint64(0))

    pos, err = log.CheckTail(true)
    assert.NoError(t, err)
    assert.Equal(t, pos, uint64(1))

    pos, err = log.CheckTail(true)
    assert.NoError(t, err)
    assert.Equal(t, pos, uint64(2))

    pos, err = log.CheckTail(false)
    assert.NoError(t, err)
    assert.Equal(t, pos, uint64(2))

    log.Destroy()

    pool.Destroy()
    conn.Shutdown()
}

func TestAppend(t *testing.T) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	conn.Connect()

	poolname := GetUUID()
	err := conn.MakePool(poolname)
	assert.NoError(t, err)

	pool, err := conn.OpenIOContext(poolname)
	assert.NoError(t, err)

    log, err := zlog.Create(pool, "mylog", 5, "localhost", "5678")
    assert.NoError(t, err)

	data := []byte("input data")

    var last uint64 = 0
    for i := 0; i < 100; i++ {
        pos, err := log.Append(data)
        assert.NoError(t, err)

        assert.True(t, pos > last)
        last = pos

        tail, err := log.CheckTail(false)
        assert.NoError(t, err)
        assert.Equal(t, pos, tail)
    }

    log.Destroy()

    pool.Destroy()
    conn.Shutdown()
}

func TestFill(t *testing.T) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	conn.Connect()

	poolname := GetUUID()
	err := conn.MakePool(poolname)
	assert.NoError(t, err)

	pool, err := conn.OpenIOContext(poolname)
	assert.NoError(t, err)

    log, err := zlog.Create(pool, "mylog", 5, "localhost", "5678")
    assert.NoError(t, err)

    err = log.Fill(0)
    assert.NoError(t, err)

    err = log.Fill(232)
    assert.NoError(t, err)

    err = log.Fill(232)
    assert.NoError(t, err)

	data := []byte("input data")
    pos, err := log.Append(data)
    assert.NoError(t, err)

    err = log.Fill(pos)
    assert.Error(t, err, "Read only")

    log.Destroy()

    pool.Destroy()
    conn.Shutdown()
}

func TestRead(t *testing.T) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	conn.Connect()

	poolname := GetUUID()
	err := conn.MakePool(poolname)
	assert.NoError(t, err)

	pool, err := conn.OpenIOContext(poolname)
	assert.NoError(t, err)

    log, err := zlog.Create(pool, "mylog", 5, "localhost", "5678")
    assert.NoError(t, err)

    buf := make([]byte, 4096)

    size, err := log.Read(0, buf)
    assert.Error(t, err, "Not written")

    err = log.Fill(0)
    assert.NoError(t, err)

    size, err = log.Read(0, buf)
    assert.Error(t, err, "Filled")

    size, err = log.Read(232, buf)
    assert.Error(t, err, "Not written")

    err = log.Fill(232)
    assert.NoError(t, err)

    size, err = log.Read(232, buf)
    assert.Error(t, err, "Filled")

    bytes_in := []byte("this is a string")
    pos, err := log.Append(bytes_in)
    assert.NoError(t, err)

    bytes_out := make([]byte, len(bytes_in))
    assert.Equal(t, len(bytes_in), len(bytes_out))
    assert.NotEqual(t, bytes_in, bytes_out)

    size, err = log.Read(pos, bytes_out)
    assert.Equal(t, size, len(bytes_in))
    assert.Equal(t, bytes_in, bytes_out)

    log.Destroy()

    pool.Destroy()
    conn.Shutdown()
}
