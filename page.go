package rmdb

import (
	"bytes"
	"encoding/binary"
)

type Page struct {
	Id             uint64
	columns        []Column
	lines          map[uint64]Line
	max            uint64
	Offset, Length uint64
	isDirty        bool
}

type Memtable struct {
	lines            map[uint64]Line
	isSwap, isOrigin bool //origin原来的
}

func (l *LruCache) NewPage() (*Page, error) {
	page := &Page{
		Id:      l.pageId,
		columns: l.table.Columns,
		lines:   make(map[uint64]Line, 16),
		max:     l.maxLine,
		Length:  0,
		isDirty: true,
	}
	err := l.AddPage(page)
	if err != nil {
		return nil, err
	}
	l.pageId++
	l.pageNum++
	return page, nil
}

func (p *Page) InsertLine(line Line) bool {
	for i := uint64(0); i < p.max; i++ { // 找位置插入
		if len(p.lines[i].nameToVal) == 0 { // 如果p.lines[i]为空
			line.pageId = p.Id
			line.lineId = i
			p.lines[i] = line
			p.isDirty = true
			return true
		}
	}
	return false
}



func (p *Page) EncodePage() []byte {
	if len(p.lines) == 0 {
		return []byte{}
	}
	buf := new(bytes.Buffer)
	for i := uint64(0); i < p.max; i++ {
		if line, ok := p.lines[i]; ok {
			data := p.EncodeLine(line)
			length := make([]byte, 8)
			binary.LittleEndian.PutUint64(length, uint64(len(data)))
			buf.Write(length)
			buf.Write(data)
		}
	}
	return buf.Bytes()
}

func (p *Page) DecodePage(data []byte) {
	offset := uint64(0)
	for offset < uint64(len(data)) {
		lendata := data[offset : offset+8]
		length := binary.LittleEndian.Uint64(lendata)
		offset += 8
		lineData := data[offset : offset+length]
		line := p.DecodeLine(lineData)
		p.InsertLine(line)
		offset += length
	}
}

func (p *Page) EncodeLine(line Line) []byte {
	buf := new(bytes.Buffer)
	for _, column := range p.columns {
		length := make([]byte, 8)
		val := line.nameToVal[column.Name].value
		binary.LittleEndian.PutUint64(length, uint64(len(val)))
		buf.Write(length)
		buf.Write(val)
	}
	return buf.Bytes()
}

func (p *Page) DecodeLine(data []byte) Line {
	vals := make([][]byte, 0, 16)
	offset := uint64(0)
	for offset < uint64(len(data)) {
		lendata := data[offset : offset+8]
		length := binary.LittleEndian.Uint64(lendata)
		offset += 8
		val := data[offset : offset+length]
		vals = append(vals, val)
		offset += length
	}
	line := Line{
		nameToVal: make(map[string]ColVal, 16), //pageid和lineid会在insert page时设置
	}
	for index, column := range p.columns {
		line.nameToVal[column.Name] = ColVal{
			column: column,
			value:  vals[index],
		}
	}
	return line
}
