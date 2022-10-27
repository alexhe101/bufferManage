package buffer

import "bufferManage/src/common"

type Replacer interface {
	Victim() (frameId common.FrameId, err error)
	Pin(frameId common.FrameId)
	Unpin(frameId common.FrameId)
	Size() uint32
}
