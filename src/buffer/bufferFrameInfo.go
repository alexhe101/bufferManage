package buffer

import "bufferManage/src/common"

type PageInfo struct {
	id common.PageId
	fd uintptr
}
