package disk

import (
	"bufferManage/src/common"
	"log"
	"os"
	"sync"
)

type DiskManager struct {
	fileName string
	file     *os.File
	latch    sync.Mutex
}

func (d *DiskManager) getFileSize() int64 {

	f, err := d.file.Stat()
	if err != nil {
		log.Fatal("error when get file size")
	}
	return f.Size()
}

func NewDiskManager(fileName string) *DiskManager {
	tmp := new(DiskManager)
	tmp.fileName = fileName
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0766)
	if err != nil {
		log.Fatal("can't open db file")
	}
	tmp.file = file
	return tmp
}

// WritePage writes page data to disk,page data should be slice/**
func (d *DiskManager) WritePage(pageId common.PageId, pageData []byte) {
	d.latch.Lock()
	defer d.latch.Unlock()
	offset := pageId * common.PAGESIZE
	d.file.Seek(int64(offset), 0)
	_, err := d.file.Write(pageData)
	if err != nil {
		log.Fatal("write error")
	}
	d.file.Sync()
}

func (d *DiskManager) ReadPage(pageId common.PageId, pageData []byte) {
	d.latch.Lock()
	defer d.latch.Unlock()
	offset := int64(pageId * common.PAGESIZE)
	if offset > d.getFileSize() {
		log.Println("")
	} else {
		d.file.Seek(offset, 0)
		readCount, err := d.file.Read(pageData)
		if err != nil {
			log.Fatal("error while read")
		}
		if readCount < common.PAGESIZE {
			log.Println("read less than a page")
		}
	}
}

func (d *DiskManager) ShutDown() {
	d.latch.Lock()
	defer d.latch.Unlock()
	d.file.Close()
}
