package buffer

import (
	"bufferManage/src/common"
	storage "bufferManage/src/storage/page"
	"container/list"
	"sync"
)

// MemPool 需要设计并发控制 让一个frameID只能归属一个bufferpool
type MemPool struct {
	pages            []*storage.Page
	replacer         Replacer
	freeList         list.List
	latch            sync.Mutex
	pagetable        map[PageInfo]common.FrameId
	inversePageTable map[common.FrameId]PageInfo
}

func NewMemPool(poolSize uint) *MemPool {
	var t MemPool
	t.replacer = NewLruReplacer(poolSize)
	t.pages = make([]*storage.Page, poolSize)
	for i := 0; i < len(t.pages); i++ {
		t.pages[i] = storage.NewPage()
		t.freeList.PushBack(common.FrameId(i))
	}
	t.pagetable = make(map[PageInfo]common.FrameId, poolSize)
	t.inversePageTable = make(map[common.FrameId]PageInfo, poolSize)
	return &t
}

func (m *MemPool) GetFreeIndex() (frameID common.FrameId, err error) {
	m.latch.Lock()
	defer m.latch.Unlock()
	idx := m.freeList.Front()
	if idx != nil {
		frameID = idx.Value.(common.FrameId)
		m.freeList.Remove(idx)
		return frameID, nil

	}
	frameID, err = m.replacer.Victim()
	if err == nil {
		info, ok := m.inversePageTable[frameID]
		if ok {
			delete(m.pagetable, info)
			delete(m.inversePageTable, frameID)
		}
	}
	return frameID, err
}

//frameid仅归属于唯一的diskbufferpool
func (m *MemPool) FetchPage(id common.FrameId) *storage.Page {
	return m.pages[id]
}

func (m *MemPool) Pin(id common.FrameId) {
	m.replacer.Pin(id)
}

func (m *MemPool) Unpin(id common.FrameId) {
	m.replacer.Unpin(id)
}

func (m *MemPool) ResetPage(id common.FrameId) {
	m.pages[id].SetPageId(common.INVALIDPAGEID)
	m.pages[id].SetDirty(false)
	m.pages[id].ResetMemory()
	m.pages[id].SetPinCount(0)
}

func (m *MemPool) SetInfo(info PageInfo, id common.FrameId) {
	m.pagetable[info] = id
	m.inversePageTable[id] = info
}

func (m *MemPool) DeletePage(info PageInfo, id common.FrameId) {
	delete(m.pagetable, info)
	delete(m.inversePageTable, id)
	m.ResetPage(id)
	m.freeList.PushBack(id)
}
