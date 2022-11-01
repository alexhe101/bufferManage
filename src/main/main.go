package main

//slice作为参数
func Test1(arr []int) {
	for i := 0; i < len(arr); i++ {
		arr[i] += 1
	}
}

func Test2(arr [5]int) {
	for i := 0; i < len(arr); i++ {
		arr[i] += 1
	}
}
func main() {
	arr := [5]int{1, 2, 3, 4, 5}
	//数组无法传入slice
	//Test1(arr)
	arr2 := []int{1, 2, 3, 4, 5}
	//slice 可传入数组
	Test1(arr2)
	//数组可传入数组
	Test2(arr)
	//slice无法传入数组
	//Test2(arr2)
}
