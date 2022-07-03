package hdpsr

import "math/rand"

type sortNode struct {
	diskId  int
	idx     int
	blockId int
	latency float64
}

type qsort struct {
	arr []*sortNode
}

func (qs *qsort) swap(i, j int) {
	qs.arr[i], qs.arr[j] = qs.arr[j], qs.arr[i]
}

func (qs *qsort) quickSort(l, r, k int) {
	if l >= r {
		return
	}
	i := l
	j := r
	ridx := rand.Intn(r-l+1) + l
	qs.swap(ridx, l)
	x := qs.arr[l].latency
	for i < j {
		for i < j && qs.arr[j].latency <= x {
			j--
		}
		for i < j && qs.arr[i].latency >= x {
			i++
		}
		qs.swap(i, j)
	}
	qs.swap(i, l)
	if i > k {
		qs.quickSort(l, i-1, k)
	}
	if i < k {
		qs.quickSort(i+1, r, k)
	}
}

func BiggestK(arr []*sortNode, k int) []*sortNode {
	if k == 0 {
		return nil
	}
	n := len(arr)
	if k > n {
		k = n
	}
	qs := &qsort{arr: arr}
	qs.quickSort(0, n-1, k)
	return qs.arr[0:k]
}
