package common

type PageId int32
type FrameId int32

const PAGESIZE = 4096
const INVALIDPAGEID PageId = -1
const INTSIZE = 4
const BitMapSize = PAGESIZE - INTSIZE - INTSIZE
const MAXPAGE = BitMapSize * 8
