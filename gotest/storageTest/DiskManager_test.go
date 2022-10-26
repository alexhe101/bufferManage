package DiskManager_test

import (
	"bufferManage/src/common"
	"bufferManage/src/storage/disk"
	"reflect"
	"testing"
)

func TestReadWritePage(t *testing.T) {
	dbString := "test.db"
	pageData := make([]byte, common.PAGESIZE)
	cmpData := make([]byte, common.PAGESIZE)
	copy(cmpData, []byte("A test string."))
	diskManager := disk.NewDiskManager(dbString)
	defer diskManager.ShutDown()
	//cmpData[:]=
	diskManager.ReadPage(0, pageData)
	diskManager.WritePage(0, cmpData)
	diskManager.ReadPage(0, pageData)
	if !reflect.DeepEqual(pageData, cmpData) {
		t.Errorf("two data not equal %#v,%#v", pageData, cmpData)
	}
	pageData = make([]byte, common.PAGESIZE)
	diskManager.WritePage(5, cmpData)
	diskManager.ReadPage(5, pageData)
	if !reflect.DeepEqual(pageData, cmpData) {
		t.Errorf("two data not equal %#v,%#v", pageData, cmpData)
	}

}
