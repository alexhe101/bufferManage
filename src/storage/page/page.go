package storage

import (
	"bufferManage/src/common"
	"sync"
)

type Page struct {
	isDirty        bool
	readWriteLatch sync.RWMutex
	pinCount       int
	pageId         common.PageId
	data           []byte //不声明长度就是slice
}

func (p *Page) IsDirty() bool {
	return p.isDirty
}

func (p *Page) SetDirty(isDirty bool) {
	p.isDirty = isDirty
}

func (p *Page) PinCount() int {
	return p.pinCount
}

func (p *Page) SetPinCount(pinCount int) {
	p.pinCount = pinCount
}

func (p *Page) PageId() common.PageId {
	return p.pageId
}

func (p *Page) SetPageId(pageId common.PageId) {
	p.pageId = pageId
}

func (p *Page) Data() []byte {
	return p.data
}

func (p *Page) ResetMemory() {
	p.data = make([]byte, common.PAGESIZE)
}

func NewPage() *Page {
	t := new(Page)
	t.data = make([]byte, common.PAGESIZE)
	t.pageId = common.INVALIDPAGEID
	return t
}

func (p *Page) IncPinCount() {
	p.pinCount += 1
}

func (p *Page) DecPinCount() {
	p.pinCount -= 1
}
