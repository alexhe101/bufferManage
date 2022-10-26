package disk

type DiskManager struct {
	fileName string
}

func NewDiskManager(fileName string) *DiskManager {
	return &DiskManager{fileName: fileName}
}
