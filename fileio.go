package rmdb

import (
	"errors"
	"golangProject/kvdb/rmdb_beta/mmap"
	"io"
	"os"
)

type FileIO interface {
	Write(b []byte) (n int, err error)
	ReadAt(b []byte, off int64) (n int, err error)
	Name() string
	Sync() error
	Close() error
}

type MMap struct {
	fd             *os.File
	buf            []byte // a buffer of mmap
	bufLen, offset int64
}

func OpenFile(path string) (FileIO, error) {
	switch GlobalOption.IOMode {
	case Standard:
		file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		return file, nil
	case MMapMode:
		return NewMMap(path, GlobalOption.MmapSize) // 这里的size取决于最后的数据库文件大小
	}
	return nil, errors.New("invaild io mode")
}

func NewMMap(path string, size int64) (FileIO, error) {
	if size <= 0 {
		return nil, errors.New("invalid fsize")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() < size {
		if err := file.Truncate(size); err != nil {
			return nil, err
		}
	}
	buf, err := mmap.Mmap(file, true, size)
	if err != nil {
		return nil, err
	}
	return &MMap{
		fd:     file,
		buf:    buf,
		bufLen: int64(len(buf)),
		offset: int64(0),
	}, nil
}

func (m *MMap) Write(b []byte) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	n := copy(m.buf[m.offset:], b)
	m.offset += int64(len(b))
	return n, nil
}

func (m *MMap) ReadAt(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= m.bufLen {
		return 0, io.EOF
	}
	if offset+int64(len(b)) >= m.bufLen {
		return 0, io.EOF
	}
	return copy(b, m.buf[offset:]), nil
}

func (m *MMap) Name() string {
	return m.fd.Name()
}

func (m *MMap) Sync() error {
	return mmap.Msync(m.buf)
}

func (m *MMap) Close() error {
	if err := mmap.Msync(m.buf); err != nil {
		return err
	}
	if err := mmap.Munmap(m.buf); err != nil {
		return err
	}
	err := m.fd.Truncate(m.offset)
	if err != nil {
		return err
	}
	return m.fd.Close()
}

func (m *MMap) Delete() error {
	if err := mmap.Munmap(m.buf); err != nil {
		return err
	}
	m.buf = nil

	if err := m.fd.Truncate(0); err != nil {
		return err
	}
	if err := m.fd.Close(); err != nil {
		return err
	}
	return os.Remove(m.fd.Name())
}
