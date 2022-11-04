package DiskManager

import (
	"bufferManage/src/buffer"
	"bufferManage/src/common"
	"bufferManage/src/storage/disk"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func EXPECT_IDEQ(a int, c common.PageId, t *testing.T) {
	b := int(c)
	if a != b {
		t.Errorf("EXPECT EQ %#v,%#v", a, b)
	}
}
func TestSample(t *testing.T) {
	db_name := "test.db"
	buffer_pool_size := 10
	diskmanager := disk.NewDiskManager(db_name)
	memPool := buffer.NewMemPool(uint(buffer_pool_size))
	bpm := buffer.NewBufferPoolManagerInstance(memPool, diskmanager)

	page0 := bpm.CreateNewPage()
	EXPECT_IDEQ(1, page0.PageId(), t)

	copy(page0.Data(), []byte("Hello"))
	var s string
	s = string(page0.Data()[:len("Hello")])
	if s != "Hello" {
		t.Errorf("not equal")
	}

	for i := 1; i < buffer_pool_size-1; i++ {
		page0 = bpm.CreateNewPage()
		if page0 == nil {
			t.Errorf("")
		}
	}

	for i := buffer_pool_size; i < buffer_pool_size*2; i++ {
		page0 = bpm.CreateNewPage()
		if page0 != nil {
			t.Error()
		}
	}

	for i := 1; i < 6; i++ {
		if bpm.UnPinPage(common.PageId(i), true) != true {
			t.Error()
		}
	}

	for i := 1; i < 5; i++ {
		page0 = bpm.CreateNewPage()
		if page0 == nil {
			t.Error()
		}
	}

	page0 = bpm.FetchPage(1)

	s = string(page0.Data()[:len("Hello")])
	if s != "Hello" {
		t.Errorf("not equal")
	}

	bpm.UnPinPage(1, true)
	page0 = bpm.CreateNewPage()
	if page0 == nil {
		t.Error()
	}

	page0 = bpm.CreateNewPage()
	if page0 != nil {
		t.Error()
	}

	page0 = bpm.FetchPage(1)
	if page0 != nil {
		t.Error()
	}

	diskmanager.ShutDown()
	os.Remove("test.db")
}

func TestBinaryData(t *testing.T) {
	db_name := "test.db"
	buffer_pool_size := 10
	diskmanager := disk.NewDiskManager(db_name)
	memPool := buffer.NewMemPool(uint(buffer_pool_size))
	bpm := buffer.NewBufferPoolManagerInstance(memPool, diskmanager)
	page0 := bpm.CreateNewPage()
	EXPECT_IDEQ(1, page0.PageId(), t)
	randomData := make([]byte, common.PAGESIZE)
	rand.Seed(15645)
	for i := 0; i < len(randomData); i++ {
		randomData[i] = byte(rand.Int() % 256)
	}
	randomData[common.PAGESIZE/2] = '0'
	randomData[common.PAGESIZE-1] = '0'
	copy(page0.Data(), randomData)
	if string(page0.Data()) != string(randomData) {
		t.Error()
	}
	for i := 1; i < buffer_pool_size-1; i++ {
		page0 = bpm.CreateNewPage()
		if page0 == nil {
			t.Errorf("")
		}
	}

	for i := buffer_pool_size; i < buffer_pool_size*2; i++ {
		page0 = bpm.CreateNewPage()
		if page0 != nil {
			t.Error()
		}
	}

	for i := 1; i < 6; i++ {
		if bpm.UnPinPage(common.PageId(i), true) != true {
			t.Error()
		}
	}

	for i := 1; i < 6; i++ {
		page0 = bpm.CreateNewPage()
		if page0 == nil {
			t.Error()
		}
		bpm.UnPinPage(page0.PageId(), false)
	}

	page0 = bpm.FetchPage(1)
	if string(page0.Data()) != string(randomData) {
		t.Error()
	}

	diskmanager.ShutDown()
	os.Remove("test.db")
}

func TestNewPage(t *testing.T) {
	db_name := "test.db"
	buffer_pool_size := 10
	diskmanager := disk.NewDiskManager(db_name)
	memPool := buffer.NewMemPool(uint(buffer_pool_size))
	bpm := buffer.NewBufferPoolManagerInstance(memPool, diskmanager)
	pageIDs := make([]int, 0)
	for i := 1; i < 10; i++ {
		page := bpm.CreateNewPage()
		if page == nil {
			t.Error()
		}
		copy(page.Data(), fmt.Sprintf("%d", i))
		pageIDs = append(pageIDs, i)
	}

	for i := 0; i < 100; i++ {
		page := bpm.CreateNewPage()
		if page != nil {
			t.Error()
		}
	}

	for i := 0; i < 5; i++ {
		if bpm.UnPinPage(common.PageId(pageIDs[i]), true) != true {
			t.Error()
		}
	}

	for i := 0; i < 5; i++ {
		page := bpm.CreateNewPage()
		if page == nil {
			t.Error()
		}
		pageIDs[i] = int(page.PageId())
	}

	for i := 0; i < 100; i++ {
		page := bpm.CreateNewPage()
		if page != nil {
			t.Error()
		}
	}

	for i := 0; i < 5; i++ {
		if bpm.UnPinPage(common.PageId(pageIDs[i]), false) != true {
			t.Error()
		}
	}
	for i := 0; i < 5; i++ {
		page := bpm.CreateNewPage()
		if page == nil {
			t.Error()
		}
	}

	for i := 0; i < 100; i++ {
		page := bpm.CreateNewPage()
		if page != nil {
			t.Error()
		}
	}
	diskmanager.ShutDown()
	os.Remove("test.db")

}

func TestName(t *testing.T) {
	db_name := "test.db"
	buffer_pool_size := 100
	diskmanager := disk.NewDiskManager(db_name)
	memPool := buffer.NewMemPool(uint(buffer_pool_size))
	bpm := buffer.NewBufferPoolManagerInstance(memPool, diskmanager)
	res := make([]int, 0)
	//1 .测试是否正常分配
	for i := 0; i < 99; i++ {
		res = append(res, int(bpm.CreateNewPage().PageId()))
	}

	for i := 0; i < 99; i++ {
		if res[i] != i+1 {
			t.Error()
		}
	}

	//测试删除情况
	for i := 0; i < 5; i++ {
		bpm.UnPinPage(common.PageId(i+1), false)
		bpm.DeletePage(common.PageId(i + 1))
		if bpm.Header().IsAllocated(i+1) == true {
			t.Error()
		}
	}

	if bpm.Header().PageCount() != 100 {
		t.Error()
	}

	if bpm.Header().AllocateCount() != 95 {
		t.Error()
	}

	//情况上述数据
	for i := 5; i < 99; i++ {
		bpm.UnPinPage(common.PageId(i+1), false)
		bpm.DeletePage(common.PageId(i + 1))
	}

	if bpm.Header().AllocateCount() != 1 {
		t.Error()
	}
	//测试插入页数已满

	var limit int = common.MAXPAGE
	for i := 0; i < limit-1; i++ {
		tmp := bpm.CreateNewPage()
		if tmp == nil {
			t.Error()
		}
		bpm.UnPinPage(tmp.PageId(), false)
	}
	bpm.FlushPage(4000)
	if bpm.CreateNewPage() != nil {
		t.Error()
	}

	bpm.DeletePage(5)
	bpm.DeletePage(6)
	bpm.DeletePage(7)

	page := bpm.CreateNewPage()
	if page.PageId() != 5 {
		t.Error()
	}
	page = bpm.CreateNewPage()
	if page.PageId() != 6 {
		t.Error()
	}
	page = bpm.CreateNewPage()
	if page.PageId() != 7 {
		t.Error()
	}
	bpm.FlushAllPage()

	diskmanager.ShutDown()

	//3. 测试重启情况

	//4. 测试已满分配
	diskmanager = disk.NewDiskManager(db_name)
	memPool = buffer.NewMemPool(uint(buffer_pool_size))
	bpm = buffer.NewBufferPoolManagerInstance(memPool, diskmanager)
	if bpm.Header().PageCount() != common.MAXPAGE {
		t.Error()
	}

	if bpm.Header().AllocateCount() != common.MAXPAGE {
		t.Error()
	}
	diskmanager.ShutDown()
	os.Remove(db_name)
}

func TestUnPin(t *testing.T) {
	db_name := "test.db"
	buffer_pool_size := 3
	diskmanager := disk.NewDiskManager(db_name)
	memPool := buffer.NewMemPool(uint(buffer_pool_size))
	bpm := buffer.NewBufferPoolManagerInstance(memPool, diskmanager)
	page0 := bpm.CreateNewPage()
	copy(page0.Data(), "page0")
	page1 := bpm.CreateNewPage()
	copy(page1.Data(), "page1")

	bpm.UnPinPage(page0.PageId(), true)
	bpm.UnPinPage(page1.PageId(), true)

	for i := 0; i < 2; i++ {
		tmp := bpm.CreateNewPage()
		if tmp == nil {
			t.Error()
		}
		bpm.UnPinPage(tmp.PageId(), true)
	}

	page := bpm.FetchPage(1)
	if string(page.Data()[:len("page0")]) != "page0" {
		t.Error()
	}

	copy(page.Data(), "page0updated")
	page = bpm.FetchPage(2)
	if string(page.Data()[:len("page1")]) != "page1" {
		t.Error()
	}
	copy(page.Data(), "page1updated")
	bpm.UnPinPage(1, false)
	bpm.UnPinPage(2, true)
	for i := 0; i < 2; i++ {
		tmp := bpm.CreateNewPage()
		if tmp == nil {
			t.Error()
		}
		bpm.UnPinPage(tmp.PageId(), true)
	}
	page = bpm.FetchPage(1)
	if string(page.Data()[:len("page0")]) != "page0" {
		t.Error()
	}
	page = bpm.FetchPage(2)
	if string(page.Data()[:len("page1updated")]) != "page1updated" {
		t.Error()
	}
}

func TestDirty(t *testing.T) {
	db_name := "test.db"
	buffer_pool_size := 2
	diskmanager := disk.NewDiskManager(db_name)
	memPool := buffer.NewMemPool(uint(buffer_pool_size))
	bpm := buffer.NewBufferPoolManagerInstance(memPool, diskmanager)
	page0 := bpm.CreateNewPage()
	copy(page0.Data(), "page0")
	bpm.UnPinPage(1, true)
	page0 = bpm.FetchPage(page0.PageId())
	if page0.IsDirty() != true {
		t.Error()
	}
	bpm.UnPinPage(1, false)

	page0 = bpm.FetchPage(page0.PageId())
	if page0.IsDirty() != true {
		t.Error()
	}
	bpm.UnPinPage(1, false)
	page := bpm.CreateNewPage()
	if page.IsDirty() {
		t.Error()
	}
	copy(page.Data(), "page")
	bpm.UnPinPage(page.PageId(), true)
	if page.IsDirty() == false {
		t.Error()
	}
	bpm.DeletePage(page.PageId())
	diskmanager.ShutDown()
	os.Remove(db_name)
}
