package buffer

import (
	"bufferManage/src/common"
	"bufferManage/src/storage/disk"
	storage "bufferManage/src/storage/page"
	"bytes"
	"encoding/binary"
	"reflect"
	"sync"
	"unsafe"
)

type BufferPoolManagerInstance struct {
	diskManager *disk.DiskManager
	buffer      *MemPool
	header      *HeapHeader
	latch       sync.Mutex
}

func (b *BufferPoolManagerInstance) Header() *HeapHeader {
	return b.header
}

// Pages 返回缓冲区
func (b *BufferPoolManagerInstance) Pages() []*storage.Page {
	return b.buffer.pages
}

func NewBufferPoolManagerInstance(memPool *MemPool, diskManager *disk.DiskManager) *BufferPoolManagerInstance {
	var t BufferPoolManagerInstance
	t.diskManager = diskManager
	t.buffer = memPool
	page := t.FetchPage(0)
	b := page.Data()
	t.header = (*HeapHeader)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&b)).Data))
	if t.header.pageCount == 0 {
		t.header.allocateCount = 1
		t.header.pageCount = 1
		t.header.SetAllocated(0)
	}
	return &t
}

// FetchPage 返回空闲的bucket 可用来装入一个硬盘页
func (b *BufferPoolManagerInstance) FetchPage(id common.PageId) *storage.Page {
	b.latch.Lock()
	defer b.latch.Unlock()
	fd := b.diskManager.Fd()
	info := PageInfo{id, fd}

	//如果这一页没牺牲，或是没有被delete，则还能继续用，直接从lru里拿出来
	//如果在lru里 刚好被淘汰了怎么办？
	//把fetch这一步也移动到memory manager去
	//有disk manager 不好移动进去
	frameID, ok := b.buffer.pagetable[info]

	if ok {
		page := b.buffer.FetchPage(frameID)
		page.IncPinCount()
		b.buffer.Pin(frameID)
		return page
	}

	//如果已经被替换 或被删除 则获取一个新页
	frameID, err := b.buffer.GetFreeIndex()

	if err == nil {
		b.buffer.Pin(frameID)
		page := b.buffer.FetchPage(frameID)
		//被淘汰的时候 判断是否dirty 若是则写入
		if page.IsDirty() {
			b.diskManager.WritePage(page.PageId(), page.Data())
		}
		page.ResetMemory()
		b.diskManager.ReadPage(id, page.Data())
		page.SetPinCount(1)
		page.SetPageId(id)
		page.SetDirty(false)
		b.buffer.SetInfo(info, frameID)
		return page
	}
	return nil
}

//表明一个页暂时不使用
func (b *BufferPoolManagerInstance) UnPinPage(id common.PageId, isDirty bool) bool {
	b.latch.Lock()
	defer b.latch.Unlock()
	fd := b.diskManager.Fd()
	info := PageInfo{id, fd}

	frameID, ok := b.buffer.pagetable[info]
	//已经被删了或是被替换了
	if !ok {
		return false
	}

	page := b.buffer.FetchPage(frameID)

	//已经早已unpin 但是还没被替换或删除
	if page.PinCount() <= 0 {
		return false
	}

	//正常情况
	page.DecPinCount()
	if isDirty {
		page.SetDirty(isDirty)
	}
	if page.PinCount() == 0 {
		//扔到lrucache里 但保存在pagetable中
		b.buffer.Unpin(frameID)
	}

	return true

}

func (b *BufferPoolManagerInstance) FlushPage(id common.PageId) {
	b.latch.Lock()
	defer b.latch.Unlock()
	fd := b.diskManager.Fd()
	info := PageInfo{id, fd}
	frameId := b.buffer.pagetable[info]

	b.diskManager.WritePage(id, b.buffer.pages[frameId].Data())
	b.buffer.pages[frameId].SetDirty(false)
}

func (b *BufferPoolManagerInstance) FlushAllPage() {
	for info, frameId := range b.buffer.pagetable {
		if info.fd == b.diskManager.Fd() {
			if b.buffer.pages[frameId].IsDirty() {
				b.FlushPage(info.id)
			}
		}
	}
	buf := bytes.Buffer{}
	binary.Write(&buf, binary.LittleEndian, *b.header)
	b.diskManager.WritePage(0, buf.Bytes())
}

// CreateNewPage 0.   Make sure you call AllocatePage!
// 1.   If all the pages in the buffer pool are pinned, return nullptr.
// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 3.   Update P's metadata, zero out memory and add P to the page table.
// 4.   Set the page ID output parameter. Return a pointer to P.
func (b *BufferPoolManagerInstance) CreateNewPage() *storage.Page {
	b.latch.Lock()
	defer b.latch.Unlock()
	if b.header.allocateCount >= common.MAXPAGE {
		return nil
	}

	//如果没有空闲区
	frameID, err := b.buffer.GetFreeIndex()
	if err != nil {
		return nil
	}
	//并发问题:双方同时得到同一个frameid
	//a调用pin 失败，b调用pin成功
	//此时a再次调用pin
	//导致两者共享相同的frameid

	//不成立 因为getfreeindex时，已经把frameid从对应空闲区中删除了

	b.buffer.Pin(frameID)
	page := b.buffer.FetchPage(frameID)
	if page.IsDirty() {
		b.diskManager.WritePage(page.PageId(), page.Data())
	}
	page.ResetMemory()
	page.SetDirty(false)
	id := b.allocatePage()
	page.SetPageId(id)
	fd := b.diskManager.Fd()
	info := PageInfo{id, fd}
	b.buffer.SetInfo(info, frameID)
	page.SetPinCount(1)
	return b.buffer.pages[frameID]
}

func (b *BufferPoolManagerInstance) allocatePage() common.PageId {
	return b.header.AllocateNewPage()
}

func (b *BufferPoolManagerInstance) deallocatePage(id common.PageId) {
	b.header.DeallocatePage(id)
}

func (b *BufferPoolManagerInstance) DeletePage(id common.PageId) {
	b.latch.Lock()
	defer b.latch.Unlock()
	fd := b.diskManager.Fd()
	info := PageInfo{id, fd}
	frameID, ok := b.buffer.pagetable[info]
	//早已被换出或是删除
	if ok == false {
		b.deallocatePage(id)
		return
	}
	page := b.buffer.FetchPage(frameID)
	if page.PinCount() > 0 {
		return
	}

	b.deallocatePage(id)
	b.buffer.DeletePage(info, frameID)
}
