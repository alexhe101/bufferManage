package buffer

import "bufferManage/src/common"

type HeapHeader struct {
	pageCount     int32 //当前总页数
	allocateCount int32 //已分配页数
	bitmap        [common.BitMapSize]byte
}

func (h *HeapHeader) PageCount() int32 {
	return h.pageCount
}

func (h *HeapHeader) AllocateCount() int32 {
	return h.allocateCount
}

func (h *HeapHeader) SetAllocated(i int) {
	bit := i % 8
	b := i / 8
	h.bitmap[b] |= 1 << bit
}

func (h *HeapHeader) SetDeallocated(i int) {
	bit := i % 8
	b := i / 8
	h.bitmap[b] &= ^(1 << bit)
}

func (h *HeapHeader) IsAllocated(i int) bool {
	bit := i % 8
	b := i / 8
	tmp := 1 << bit
	if h.bitmap[b]&byte(tmp) == 0 {
		return false
	}
	return true
}

func (h *HeapHeader) AllocateNewPage() common.PageId {
	if h.allocateCount < h.pageCount {
		i := int32(0)
		for ; i < h.allocateCount; i++ {
			all := h.IsAllocated(int(i))
			if all == false {
				h.SetAllocated(int(i))
				h.allocateCount += 1
				return common.PageId(i)
			}
		}
	}
	idx := h.pageCount
	h.SetAllocated(int(idx))
	h.pageCount += 1
	h.allocateCount += 1
	return common.PageId(idx)
}
func (h *HeapHeader) DeallocatePage(id common.PageId) {
	idx := int(id)
	h.SetDeallocated(idx)
	h.allocateCount -= 1
}
