package rmdb

import (
	"container/list"
	"os"
	"sync"
)

type LruCache struct { // table和lrucache是一对一关系
	table            *Table
	pageList         *list.List
	pageMap          map[uint64]*list.Element
	pageId           uint64
	pageNum          uint64
	maxPage, maxLine uint64
	lock             sync.RWMutex
}

func (l *LruCache) AddPage(page *Page) error {
	p, err := l.GetPage(page.Id)
	if err != nil {
		return err
	}
	if p != nil { //说明是dirty page，此时isdirty=true
		ele := l.pageMap[page.Id]
		ele.Value = page
	} else {
		ele := l.pageList.PushFront(page)
		l.pageMap[l.pageId] = ele
	}
	for l.pageNum > l.maxPage {
		err = l.RemoveOld()
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *LruCache) RemoveOld() error {
	ele := l.pageList.Back()
	if ele != nil {
		page := ele.Value.(*Page)
		if page.isDirty {
			switch GlobalOption.IOMode {
			case Standard:
				info, err := l.table.file.(*os.File).Stat()
				if err != nil {
					return err
				}
				page.Offset = uint64(info.Size())
			case MMapMode:
				page.Offset = uint64(l.table.file.(*MMap).offset)
			}
			data := page.EncodePage()
			page.Length = uint64(len(data))
			_, err := l.table.file.Write(data)
			if err != nil {
				return err
			}
			l.table.Catalog[page.Id] = Page{
				Id:     page.Id,
				Offset: page.Offset,
				Length: page.Length,
			}
		}
		l.pageList.Remove(ele)
		delete(l.pageMap, ele.Value.(*Page).Id)
		l.pageNum--
	}
	return nil
}

func (l *LruCache) GetPage(id uint64) (*Page, error) {
	var page *Page
	if ele, ok := l.pageMap[id]; ok {
		l.pageList.MoveToFront(ele)
		page = ele.Value.(*Page)
	} else {
		if l.table.Catalog[id].Length == 0 {
			return nil, nil
		}
		page = &Page{
			Id:     l.table.Catalog[id].Id,
			Offset: l.table.Catalog[id].Offset,
			Length: l.table.Catalog[id].Length,
		}
		page.columns = l.table.Columns
		page.lines = make(map[uint64]Line, 64)
		page.max = l.maxLine
		data := make([]byte, page.Length)
		_, err := l.table.file.ReadAt(data, int64(page.Offset))
		if err != nil {
			return nil, err
		}
		page.DecodePage(data)
		page.isDirty = false
		ele := l.pageList.PushFront(page)
		l.pageMap[page.Id] = ele
		l.pageNum++
	}
	for l.pageNum > l.maxPage {
		err := l.RemoveOld()
		if err != nil {
			return nil, err
		}
	}
	return page, nil
}

func (l *LruCache) CopyPage(pageId uint64) (*Memtable, error) {
	page, err := l.GetPage(pageId)
	if err != nil {
		return nil, err
	}
	memTable := &Memtable{
		lines:    make(map[uint64]Line, 16),
		isSwap:   false,
		isOrigin: true,
	}
	for _, line := range page.lines {
		memTable.lines[line.lineId] = line
	}
	return memTable, nil
}
