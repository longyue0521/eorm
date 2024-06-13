// Copyright 2021 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package heap

import (
	"container/heap"
	"database/sql/driver"
	"reflect"

	"github.com/ecodeclub/eorm/internal/merger"
)

type Heap struct {
	h           []*Node
	sortColumns merger.SortColumns
}

func NewHeap(h []*Node, sortColumns merger.SortColumns) *Heap {
	hp := &Heap{h: h, sortColumns: sortColumns}
	heap.Init(hp)
	return hp
}

func (h *Heap) Len() int {
	return len(h.h)
}

func (h *Heap) Less(i, j int) bool {
	for k := 0; k < h.sortColumns.Len(); k++ {
		valueI := h.h[i].SortCols[k]
		valueJ := h.h[j].SortCols[k]
		_, ok := valueJ.(driver.Valuer)
		var cp func(any, any, merger.Order) int
		if ok {
			cp = merger.CompareNullable
		} else {
			kind := reflect.TypeOf(valueI).Kind()
			cp = merger.CompareFuncMapping[kind]
		}
		res := cp(valueI, valueJ, h.sortColumns.Get(k).Order)
		if res == 0 {
			continue
		}
		if res == -1 {
			return true
		}
		return false
	}
	return false
}

func (h *Heap) Swap(i, j int) {
	h.h[i], h.h[j] = h.h[j], h.h[i]
}

func (h *Heap) Push(x any) {
	h.h = append(h.h, x.(*Node))
}

func (h *Heap) Pop() any {
	v := h.h[len(h.h)-1]
	h.h = h.h[:len(h.h)-1]
	return v
}

type Node struct {
	Index    int
	SortCols []any
	Columns  []any
}
