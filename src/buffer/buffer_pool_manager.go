package buffer

import (
	"bufferManage/src/common"
	"bufferManage/src/storage/disk"
	storage "bufferManage/src/storage/page"
	"container/list"
)

type BufferPoolManagerInstance struct {
	poolSize    uint
	diskManager *disk.DiskManager
	pages       []storage.Page
	pageTable   map[common.PageId]common.FrameId
	replacer    Replacer
	freeList    list.List
}

func NewBufferPoolManagerInstance(poolSize uint, diskManager *disk.DiskManager) *BufferPoolManagerInstance {
	var t BufferPoolManagerInstance
	t.poolSize = poolSize
	t.diskManager = diskManager
	t.pages = make([]storage.Page, poolSize)
	for i := uint(0); i < poolSize; i++ {
		t.freeList.PushBack(i)
	}
	t.replacer = NewLruReplacer(poolSize)
	return &t
}

func (b BufferPoolManagerInstance) GetFreeIndex() (common.FrameId, error) {
	idx := b.freeList.Front()
	if idx != nil {
		return idx.Value.(common.FrameId), nil
	}
	return b.replacer.Victim()
}
