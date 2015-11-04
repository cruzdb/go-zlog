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
    assert.Equal(t, pos, uint64(0))

    pos, err = log.CheckTail(true)
    assert.NoError(t, err)
    assert.Equal(t, pos, uint64(1))

    pos, err = log.CheckTail(false)
    assert.NoError(t, err)
    assert.Equal(t, pos, uint64(2))

    log.Destroy()

    pool.Destroy()
    conn.Shutdown()
}

func TestCheckTailBatch(t *testing.T) {
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

    pos2 := make([]uint64, 50)

    err = log.CheckTailBatch(pos2[:1])
    assert.NoError(t, err)
    assert.Equal(t, pos2[0], uint64(0))

    err = log.CheckTailBatch(pos2[:5])
    assert.NoError(t, err)
    assert.Equal(t, pos2[0], uint64(1))
    assert.Equal(t, pos2[1], uint64(2))
    assert.Equal(t, pos2[2], uint64(3))
    assert.Equal(t, pos2[3], uint64(4))
    assert.Equal(t, pos2[4], uint64(5))

    pos, err = log.CheckTail(false)
    assert.NoError(t, err)
    assert.Equal(t, pos, uint64(6))

    pos, err = log.CheckTail(true)
    assert.NoError(t, err)
    assert.Equal(t, pos, uint64(6))

    err = log.CheckTailBatch(pos2[:2])
    assert.NoError(t, err)
    assert.Equal(t, pos2[0], uint64(7))
    assert.Equal(t, pos2[1], uint64(8))

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

    tail, err := log.CheckTail(false)
    assert.NoError(t, err)

    for i := 0; i < 100; i++ {
        pos, err := log.Append(data)
        assert.NoError(t, err)

        assert.Equal(t, pos, tail)

        tail, err = log.CheckTail(false)
        assert.NoError(t, err)
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

func TestTrim(t *testing.T) {
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

    // can trim empty spot
    err = log.Trim(55)
    assert.NoError(t, err)

    // can trim filled spot
    err = log.Fill(60)
    assert.NoError(t, err)
    err = log.Trim(60)
    assert.NoError(t, err)

    // can trim written spot
    data := []byte("input data")
    pos, err := log.Append(data)
    assert.NoError(t, err)
    err = log.Trim(pos)
    assert.NoError(t, err)

    // can trim trimmed spot
    err = log.Trim(70)
    assert.NoError(t, err)
    err = log.Trim(70)
    assert.NoError(t, err)

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

func TestMultiAppend(t *testing.T) {
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

    stream_ids := make([]uint64, 0)
    _, err = log.MultiAppend(buf, stream_ids)
    assert.Error(t, err)

    stream_ids = make([]uint64, 2)
    stream_ids[0] = 0
    stream_ids[1] = 55
    pos, err := log.MultiAppend(buf, stream_ids)
    assert.NoError(t, err)

    stream_ids_out, err := log.StreamMembership(pos)
    assert.NoError(t, err)
    assert.Equal(t, stream_ids, stream_ids_out)

    log.Destroy()

    pool.Destroy()
    conn.Shutdown()
}

func TestStreamId(t *testing.T) {
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

    stream0, err := log.OpenStream(0)
    assert.NoError(t, err)
    assert.Equal(t, 0, int(stream0.Id()))

    stream33, err := log.OpenStream(33)
    assert.NoError(t, err)
    assert.Equal(t, 33, int(stream33.Id()))

    log.Destroy()

    pool.Destroy()
    conn.Shutdown()
}

func TestStreamAppend(t *testing.T) {
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

    stream, err := log.OpenStream(0)
    assert.NoError(t, err)

    data := make([]byte, 100)
    pos1, err := stream.Append(data)
    assert.NoError(t, err)

    data_out := make([]byte, 200)
    data_out_len, pos2, err := stream.ReadNext(data_out)
    assert.Error(t, err)

    err = stream.Sync()
    assert.NoError(t, err)

    data_out_len, pos2, err = stream.ReadNext(data_out)
    assert.NoError(t, err)
    assert.Equal(t, pos1, pos2)
    assert.True(t, data_out_len > 0)
    assert.Equal(t, data, data_out[:data_out_len])

    data_out_len, pos2, err = stream.ReadNext(data_out)
    assert.Error(t, err)

    err = stream.Reset()
    assert.NoError(t, err)

    data_out_len, pos2, err = stream.ReadNext(data_out)
    assert.NoError(t, err)
    assert.Equal(t, pos1, pos2)
    assert.True(t, data_out_len > 0)
    assert.Equal(t, data, data_out[:data_out_len])

    log.Destroy()

    pool.Destroy()
    conn.Shutdown()
}
