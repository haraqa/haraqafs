package haraqafs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
)

func newTmpVolume(t testing.TB, name string) string {
	v, err := os.MkdirTemp("", name)
	checkErr(t, err)
	checkErr(t, os.MkdirAll(v, os.ModePerm))
	return v
}

func BenchmarkOS(b *testing.B) {
	const fileName = "my_file"
	v1 := newTmpVolume(b, "bench_os*")
	defer os.Remove(v1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := os.OpenFile(filepath.Join(v1, fileName), os.O_RDWR|os.O_CREATE, os.ModePerm)
		checkErr(b, err)
		checkClose(b, f)
	}
}

func BenchmarkNew(b *testing.B) {
	const fileName = "my_file"
	v1 := newTmpVolume(b, "bench_new*")
	defer os.Remove(v1)
	vol := WithVolumes(v1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := New(fileName, vol, WithCreateIfNotExist())
		checkErr(b, err)
		checkClose(b, f)
	}
}

func BenchmarkNewMulti(b *testing.B) {
	const fileName = "my_file"
	v1 := newTmpVolume(b, "bench_multi_1*")
	defer os.Remove(v1)
	v2 := newTmpVolume(b, "bench_multi_2*")
	defer os.Remove(v2)
	v3 := newTmpVolume(b, "bench_multi_3*")
	defer os.Remove(v3)
	vols := WithVolumes(v1, v2, v3)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := New(fileName, vols, WithCreateIfNotExist())
		checkErr(b, err)
		checkClose(b, f)
	}
}

func checkErr(t testing.TB, err error) {
	if err != nil {
		t.Log(string(debug.Stack()))
		t.Fatal(err)
	}
}

func TestNew(t *testing.T) {
	const fileName = "my_file"
	v1, err := os.MkdirTemp("", "vol1*")
	checkErr(t, err)
	checkErr(t, os.MkdirAll(v1, os.ModePerm))
	defer os.Remove(v1)
	f, err := New(fileName, WithVolumes(v1), WithCreate(), WithForceSync(true))
	checkErr(t, err)
	t.Log(f)
	msg := []byte("hello")
	checkWrite(t, f, msg)
	checkSeek(t, f, 0, io.SeekStart)
	checkRead(t, f, msg)
	checkClose(t, f)

	f, err = New(fileName, WithVolumes(v1), WithCreateIfNotExist())
	checkErr(t, err)
	t.Log(f)
	checkSeek(t, f, 0, io.SeekStart)
	checkRead(t, f, msg)
	checkClose(t, f)

	v2 := filepath.Join(os.TempDir(), "vol2*")
	checkErr(t, os.MkdirAll(v2, os.ModePerm))
	defer os.Remove(v2)
	f, err = New(fileName, WithVolumes(v1, v2), WithCreateIfNotExist(), WithQuorum(1))
	checkErr(t, err)
	checkRead(t, f, msg)
	checkClose(t, f)

	f, err = New(fileName, WithVolumes(v2), WithCreateIfNotExist())
	checkErr(t, err)
	checkRead(t, f, msg)
	checkClose(t, f)
}

func checkWrite(t *testing.T, f *File, msg []byte) {
	n, err := f.Write(msg)
	checkErr(t, err)
	if n != len(msg) {
		t.Fatal(n)
	}
}
func checkRead(t *testing.T, f *File, expect []byte) {
	msg := make([]byte, len(expect))
	n, err := f.Read(msg)
	if err != nil && !errors.Is(err, io.EOF) {
		checkErr(t, err)
	}
	if n != len(expect) {
		t.Log(string(debug.Stack()))
		t.Fatal(n)
	}
	if !bytes.Equal(expect, msg) {
		t.Log(expect)
		t.Fatal(msg)
	}
}
func checkSeek(t *testing.T, f *File, offset int64, whence int) {
	_, err := f.Seek(offset, whence)
	checkErr(t, err)
}
func checkClose(t testing.TB, f io.Closer) {
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
