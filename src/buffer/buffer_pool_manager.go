package buffer

import (
	"bufferManage/src/common"
	"bufferManage/src/storage/disk"
	storage "bufferManage/src/storage/page"
	"container/list"
	"sync"
	"sync/atomic"
)

type BufferPoolManagerInstance struct {
	poolSize    uint
	diskManager *disk.DiskManager
	pages       []*storage.Page
	pageTable   map[common.PageId]common.FrameId
	replacer    Replacer
	freeList    list.List
	latch       sync.Mutex
	nextPageID  int64
}

// PoolSize 返回bufferpool的大小
func (b *BufferPoolManagerInstance) PoolSize() uint {
	return b.poolSize
}

// Pages 返回缓冲区
func (b *BufferPoolManagerInstance) Pages() []*storage.Page {
	return b.pages
}

func NewBufferPoolManagerInstance(poolSize uint, diskManager *disk.DiskManager) *BufferPoolManagerInstance {
	var t BufferPoolManagerInstance
	t.poolSize = poolSize
	t.diskManager = diskManager
	t.pages = make([]*storage.Page, poolSize)
	for i := 0; uint(i) < poolSize; i++ {
		t.pages[i] = storage.NewPage()
	}
	for i := uint(0); i < poolSize; i++ {
		t.freeList.PushBack(i)
	}
	t.replacer = NewLruReplacer(poolSize)
	return &t
}

// GetFreeIndex 找出空闲的index 若不存在 则返回nil
func (b *BufferPoolManagerInstance) GetFreeIndex() (frameID common.FrameId, err error) {
	idx := b.freeList.Front()
	b.freeList.Remove(idx)
	if idx != nil {
		return idx.Value.(common.FrameId), nil
	}
	return b.replacer.Victim()
}

// FetchPage 返回空闲的bucket 可用来装入一个硬盘页
func (b *BufferPoolManagerInstance) FetchPage(id common.PageId) *storage.Page {
	b.latch.Lock()
	defer b.latch.Unlock()
	frameID, ok := b.pageTable[id]
	if ok {
		b.pages[frameID].IncPinCount()
		b.replacer.Pin(frameID)
		return b.pages[frameID]
	}
	frameID, err := b.GetFreeIndex()
	if err != nil {
		if b.pages[frameID].IsDirty() {
			b.diskManager.WritePage(b.pages[frameID].PageId(), b.pages[frameID].Data())
		}
		b.pages[frameID].ResetMemory()
		b.pages[frameID].IncPinCount()
		b.pages[frameID].SetPageId(id)
		b.pages[frameID].SetDirty(false)
		b.pageTable[id] = frameID
		return b.pages[frameID]
	}
	return nil
}

//表明一个页暂时不使用
func (b *BufferPoolManagerInstance) UnPinPage(id common.PageId, isDirty bool) {
	b.latch.Lock()
	defer b.latch.Unlock()
	frameID, ok := b.pageTable[id]
	if !ok {
		return
	}

	if b.pages[frameID].PinCount() <= 0 {
		return
	}
	b.pages[frameID].DecPinCount()
	b.pages[frameID].SetDirty(isDirty)
	if b.pages[frameID].PinCount() == 0 {
		b.replacer.Unpin(frameID)
	}
}

func (b *BufferPoolManagerInstance) FlushPage(id common.PageId) {
	b.latch.Lock()
	defer b.latch.Unlock()

	frameId := b.pageTable[id]
	b.diskManager.WritePage(id, b.pages[frameId].Data())
	b.pages[frameId].SetDirty(false)
}

func (b *BufferPoolManagerInstance) FlushAllPage() {
	for pageId, frameId := range b.pageTable {
		if b.pages[frameId].IsDirty() {
			b.FlushPage(pageId)
		}
	}
}

func (b *BufferPoolManagerInstance) CreateNewPage() *storage.Page {
	b.latch.Lock()
	defer b.latch.Unlock()
	frameID, err := b.GetFreeIndex()
	if err != nil {
		return nil
	}
	if b.pages[frameID].IsDirty() {
		b.diskManager.WritePage(b.pages[frameID].PageId(), b.pages[frameID].Data())
	}
	b.pages[frameID].ResetMemory()
	b.pages[frameID].SetDirty(false)
	id := b.allocatePage()
	b.pages[frameID].SetPageId(id)
	b.pageTable[id] = frameID
	b.replacer.Pin(frameID)
	b.pages[frameID].IncPinCount()
	return b.pages[frameID]
}

func (b *BufferPoolManagerInstance) allocatePage() common.PageId {
	ret := b.nextPageID
	atomic.AddInt64(&b.nextPageID, 1)
	return common.PageId(ret)
}
