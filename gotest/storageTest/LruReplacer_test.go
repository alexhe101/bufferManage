package DiskManager

import (
	"bufferManage/src/buffer"
	"bufferManage/src/common"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func EXPECT_EQ(a int, b int, t *testing.T) {
	if a != b {
		t.Errorf("EXPECT EQ %#v,%#v", a, b)
	}
}

func TestLruReplacer(t *testing.T) {
	lru_replacer := buffer.NewLruReplacer(7)
	lru_replacer.Unpin(1)
	lru_replacer.Unpin(2)
	lru_replacer.Unpin(3)
	lru_replacer.Unpin(4)
	lru_replacer.Unpin(5)
	lru_replacer.Unpin(6)
	lru_replacer.Unpin(1)

	if lru_replacer.Size() != 6 {
		t.Errorf("size not matched %#v 6", lru_replacer.Size())
	}
	var value common.FrameId
	value, _ = lru_replacer.Victim()
	EXPECT_EQ(1, int(value), t)
	value, _ = lru_replacer.Victim()
	EXPECT_EQ(2, int(value), t)
	value, _ = lru_replacer.Victim()
	EXPECT_EQ(3, int(value), t)

	// Scenario: pin elements in the replacer.
	// Note that 3 has already been victimized, so pinning 3 should have no effect.
	lru_replacer.Pin(3)
	lru_replacer.Pin(4)
	EXPECT_EQ(2, int(lru_replacer.Size()), t)

	// Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
	lru_replacer.Unpin(4)

	// Scenario: continue looking for victims. We expect these victims.
	value, _ = lru_replacer.Victim()
	EXPECT_EQ(5, int(value), t)
	value, _ = lru_replacer.Victim()
	EXPECT_EQ(6, int(value), t)
	value, _ = lru_replacer.Victim()
	EXPECT_EQ(4, int(value), t)

}

func TestLruVictim(t *testing.T) {
	lru_replacer := buffer.NewLruReplacer(1010)

	// Empty and try removing
	var result common.FrameId
	result, err := lru_replacer.Victim()
	if err == nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}

	// Unpin one and remove
	lru_replacer.Unpin(11)
	result, err = lru_replacer.Victim()
	if err != nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	EXPECT_EQ(11, int(result), t)

	// Unpin, remove and verify
	lru_replacer.Unpin(1)
	lru_replacer.Unpin(1)
	result, err = lru_replacer.Victim()
	if err != nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	EXPECT_EQ(1, int(result), t)
	lru_replacer.Unpin(3)
	lru_replacer.Unpin(4)
	lru_replacer.Unpin(1)
	lru_replacer.Unpin(3)
	lru_replacer.Unpin(4)
	lru_replacer.Unpin(10)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(3, int(result), t)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(4, int(result), t)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(1, int(result), t)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(10, int(result), t)
	result, err = lru_replacer.Victim()
	if err == nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}

	lru_replacer.Unpin(5)
	lru_replacer.Unpin(6)
	lru_replacer.Unpin(7)
	lru_replacer.Unpin(8)
	lru_replacer.Unpin(6)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(5, int(result), t)
	lru_replacer.Unpin(7)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(6, int(result), t)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(7, int(result), t)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(8, int(result), t)
	result, err = lru_replacer.Victim()
	if err == nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	lru_replacer.Unpin(10)
	lru_replacer.Unpin(10)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(10, int(result), t)
	result, err = lru_replacer.Victim()
	result, err = lru_replacer.Victim()
	result, err = lru_replacer.Victim()
	for i := 0; i < 1000; i++ {
		lru_replacer.Unpin(common.FrameId(i))
	}
	for i := 10; i < 1000; i++ {
		result, err = lru_replacer.Victim()
		EXPECT_EQ(i-10, int(result), t)
	}
	EXPECT_EQ(10, int(lru_replacer.Size()), t)
}

func TestPin(t *testing.T) {
	lru_replacer := buffer.NewLruReplacer(1010)

	// Empty and try removing
	var result common.FrameId
	lru_replacer.Pin(0)
	lru_replacer.Pin(1)

	// Unpin one and remove
	lru_replacer.Unpin(11)
	lru_replacer.Pin(11)
	lru_replacer.Pin(11)
	result, err := lru_replacer.Victim()
	if err == nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	lru_replacer.Pin(1)
	result, err = lru_replacer.Victim()
	if err == nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}

	// Unpin, remove and verify
	lru_replacer.Unpin(1)
	lru_replacer.Unpin(1)
	lru_replacer.Pin(1)
	result, err = lru_replacer.Victim()
	if err == nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	lru_replacer.Unpin(3)
	lru_replacer.Unpin(4)
	lru_replacer.Unpin(1)
	lru_replacer.Unpin(3)
	lru_replacer.Unpin(4)
	lru_replacer.Unpin(10)
	lru_replacer.Pin(3)
	result, err = lru_replacer.Victim()
	if err != nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	EXPECT_EQ(4, int(result), t)
	result, err = lru_replacer.Victim()
	if err != nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	EXPECT_EQ(1, int(result), t)
	result, err = lru_replacer.Victim()
	if err != nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	EXPECT_EQ(10, int(result), t)
	result, err = lru_replacer.Victim()
	if err == nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}

	lru_replacer.Unpin(5)
	lru_replacer.Unpin(6)
	lru_replacer.Unpin(7)
	lru_replacer.Unpin(8)
	lru_replacer.Unpin(6)
	lru_replacer.Pin(7)

	result, err = lru_replacer.Victim()
	if err != nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	EXPECT_EQ(5, int(result), t)

	result, err = lru_replacer.Victim()
	if err != nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	EXPECT_EQ(6, int(result), t)

	result, err = lru_replacer.Victim()
	if err != nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	EXPECT_EQ(8, int(result), t)

	result, err = lru_replacer.Victim()
	if err == nil {
		t.Errorf("Check your return value behavior for LRUReplacer::Victim")
	}
	lru_replacer.Unpin(10)
	lru_replacer.Unpin(10)
	lru_replacer.Unpin(11)
	lru_replacer.Unpin(11)
	result, err = lru_replacer.Victim()
	EXPECT_EQ(10, int(result), t)
	lru_replacer.Pin(11)

	for i := 0; i <= 1000; i++ {
		lru_replacer.Unpin(common.FrameId(i))
	}
	j := 0
	for i := 100; i < 1000; i += 2 {
		lru_replacer.Pin(common.FrameId(i))
		result, err = lru_replacer.Victim()
		if j <= 99 {
			EXPECT_EQ(j, int(result), t)
			j++
		} else {
			EXPECT_EQ(j+1, int(result), t)
			j += 2
		}
	}
	lru_replacer.Pin(result)
}

func TestIntegrate(t *testing.T) {
	var result common.FrameId
	var value_size = 10000
	lru_replacer := buffer.NewLruReplacer(uint(value_size))

	value := make([]int, value_size)
	for i := 0; i < value_size; i++ {
		value[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(value), func(i, j int) {
		value[i], value[j] = value[j], value[i]
	})

	for i := 0; i < value_size; i++ {
		lru_replacer.Unpin(common.FrameId(value[i]))
	}
	EXPECT_EQ(value_size, int(lru_replacer.Size()), t)

	// Pin and unpin 777
	lru_replacer.Pin(777)
	lru_replacer.Unpin(777)
	// Pin and unpin 0
	result, _ = lru_replacer.Victim()
	EXPECT_EQ(value[0], int(result), t)
	lru_replacer.Unpin(common.FrameId(value[0]))

	for i := 0; i < value_size/2; i++ {
		if value[i] != value[0] && value[i] != 777 {
			lru_replacer.Pin(common.FrameId(value[i]))
			lru_replacer.Unpin(common.FrameId(value[i]))
		}
	}

	lru_array := make([]int, 0)
	for i := value_size / 2; i < value_size; i++ {
		if value[i] != value[0] && value[i] != 777 {
			lru_array = append(lru_array, value[i])
		}
	}
	lru_array = append(lru_array, 777)
	lru_array = append(lru_array, value[0])
	for i := 0; i < value_size/2; i++ {
		if value[i] != value[0] && value[i] != 777 {
			lru_array = append(lru_array, value[i])
		}
	}
	EXPECT_EQ(value_size, int(lru_replacer.Size()), t)

	for i := 0; i < len(lru_array); i++ {
		result, _ = lru_replacer.Victim()
		EXPECT_EQ(lru_array[i], int(result), t)

	}
	EXPECT_EQ(value_size-len(lru_array), int(lru_replacer.Size()), t)

}

func concurr(lru *buffer.LruReplacer, value []int, tid int, part int) {
	share := 1000 / part
	for i := 0; i < share; i++ {
		lru.Unpin(common.FrameId(value[tid*share+i]))
	}
}

func TestConcurrency(t *testing.T) {
	numThreads := 50
	numRuns := 50
	for i := 0; i < numRuns; i++ {
		value_size := 1000
		lruReplacer := buffer.NewLruReplacer(uint(value_size))
		value := make([]int, value_size)
		for i := 0; i < value_size; i++ {
			value[i] = i
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(value), func(i, j int) {
			value[i], value[j] = value[j], value[i]
		})
		var wg sync.WaitGroup
		for j := 0; j < numThreads; j++ {
			wg.Add(1)
			t := j
			go func() {
				concurr(lruReplacer, value, t, numThreads)
				wg.Done()
			}()
		}
		wg.Wait()
		outvalues := make([]int, 0)
		for j := 0; j < value_size; j++ {
			result, _ := lruReplacer.Victim()
			outvalues = append(outvalues, int(result))
		}
		sort.Ints(outvalues)
		sort.Ints(value)
		if reflect.DeepEqual(outvalues, value) != true {
			t.Errorf("error in values")
		}
	}
}
