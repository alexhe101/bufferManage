package buffer

import (
	"bufferManage/src/common"
	"container/list"
	"fmt"
	"sync"
)

type LruReplacer struct {
	cap      uint
	lruCache list.List
	hash     map[common.FrameId]*list.Element
	latch    sync.Mutex
}

func NewLruReplacer(cap uint) *LruReplacer {
	l := LruReplacer{cap: cap}
	l.hash = make(map[common.FrameId]*list.Element, cap)
	return &l
}

func (l *LruReplacer) Victim() (frameId common.FrameId, err error) {
	l.latch.Lock()
	defer l.latch.Unlock()
	e := l.lruCache.Front()
	if e == nil {
		return -1, fmt.Errorf("all pinned")
	}
	frameId = e.Value.(common.FrameId)
	delete(l.hash, frameId)
	l.lruCache.Remove(e)
	return frameId, nil
}

func (l *LruReplacer) Pin(frameId common.FrameId) {
	l.latch.Lock()
	defer l.latch.Unlock()
	node, ok := l.hash[frameId]
	if ok == true {
		l.lruCache.Remove(node)
		delete(l.hash, frameId)
	}
}

func (l *LruReplacer) Unpin(frameId common.FrameId) {
	l.latch.Lock()
	defer l.latch.Unlock()
	_, ok := l.hash[frameId]
	if ok == true {
		return
	}

	e := l.lruCache.PushBack(frameId)
	l.hash[frameId] = e
}

func (l *LruReplacer) Size() uint32 {
	return uint32(l.lruCache.Len())
}
