package buffer

import (
	"bufferManage/src/common"
	"container/list"
)

type LruReplacer struct {
	cap      uint
	lruCache list.List
	hash     map[common.FrameId]*list.Element
}

func NewLruReplacer(cap uint) *LruReplacer {
	return &LruReplacer{cap: cap}
}

func (l *LruReplacer) Victim() (frameId common.FrameId, err error) {
	//TODO implement me
	panic("implement me")
}

func (l *LruReplacer) Pin(frameId common.FrameId) {
	//TODO implement me
	panic("implement me")
}

func (l *LruReplacer) Unpin(frameId common.FrameId) {
	//TODO implement me
	panic("implement me")
}

func (l *LruReplacer) Size() uint32 {
	//TODO implement me
	panic("implement me")
}
