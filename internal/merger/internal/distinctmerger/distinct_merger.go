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

package distinctmerger

import (
	"container/heap"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/ecodeclub/ekit/mapx"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	heap2 "github.com/ecodeclub/eorm/internal/merger/internal/sortmerger/heap"
	"github.com/ecodeclub/eorm/internal/rows"
	"go.uber.org/multierr"
)

type DistinctMerger struct {
	sortCols        merger.SortColumns
	hasOrderBy      bool
	distinctColumns []merger.ColumnInfo
	cols            []string
}

func NewDistinctMerger(distinctCols []merger.ColumnInfo, sortColList merger.SortColumns) (*DistinctMerger, error) {
	var sortCols merger.SortColumns
	if sortColList.IsZeroValue() {
		sortCols, _ = merger.NewSortColumns(merger.ColumnInfo{
			Name:  "TABLE",
			Order: merger.OrderDESC,
		})
		return &DistinctMerger{
			sortCols:        sortCols,
			distinctColumns: distinctCols,
		}, nil
	}
	if len(distinctCols) == 0 {
		return nil, errs.ErrDistinctColsIsNull
	}
	sortCols = sortColList
	// 检查sortCols必须全在distinctCols
	distinctMap := make(map[string]struct{})
	for _, col := range distinctCols {
		_, ok := distinctMap[col.Name]
		if ok {
			return nil, errs.ErrDistinctColsRepeated
		} else {
			distinctMap[col.Name] = struct{}{}
		}
	}
	for i := 0; i < sortCols.Len(); i++ {
		val := sortCols.Get(i)
		if _, ok := distinctMap[val.Name]; !ok {
			return nil, errs.ErrSortColListNotContainDistinctCol
		}
	}
	return &DistinctMerger{
		sortCols:        sortCols,
		distinctColumns: distinctCols,
		hasOrderBy:      true,
	}, nil
}

type key struct {
	data []any
}

func compareKey(a, b key) int {
	keyLen := len(a.data)
	for i := 0; i < keyLen; i++ {
		var cmp func(any, any, merger.Order) int
		if _, ok := a.data[i].(driver.Valuer); ok {
			cmp = merger.CompareNullable
		} else {
			cmp = merger.CompareFuncMapping[reflect.TypeOf(a.data[i]).Kind()]
		}
		res := cmp(a.data[i], b.data[i], merger.OrderASC)
		if res != 0 {
			return res
		}
	}
	return 0
}
func (o *DistinctMerger) Merge(ctx context.Context, results []*sql.Rows) (rows.Rows, error) {
	// 检测results是否符合条件
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}

	for i := 0; i < len(results); i++ {
		err := o.checkColumns(results[i])
		if err != nil {
			return nil, err
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return o.initRows(results)
}
func (o *DistinctMerger) checkColumns(rows *sql.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	// 判断数据库里的列只有去重列，且顺序要和定义的顺序一致
	if len(cols) != len(o.distinctColumns) {
		return errs.ErrDistinctColsNotInCols
	}
	for _, distinctCol := range o.distinctColumns {
		if cols[distinctCol.Index] != distinctCol.Name {
			return errs.ErrDistinctColsNotInCols
		}
	}
	if !o.hasOrderBy {
		o.cols = cols
	} else {
		o.cols, err = checkColumns(rows, cols, o.sortCols)
	}

	return err
}

func checkColumns(r *sql.Rows, cols []string, cols2 merger.SortColumns) ([]string, error) {
	return nil, nil
}

func (o *DistinctMerger) initRows(results []*sql.Rows) (*DistinctRows, error) {
	h := heap2.NewHeap(make([]*heap2.Node, 0, len(results)), o.sortCols)
	t, err := mapx.NewTreeMap[key, struct{}](compareKey)
	if err != nil {
		return nil, err
	}
	err = o.initMapAndHeap(results, t, h)
	if err != nil {
		return nil, err
	}
	return &DistinctRows{
		distinctCols: o.distinctColumns,
		rowsList:     results,
		sortCols:     o.sortCols,
		hp:           h,
		treeMap:      t,
		mu:           &sync.RWMutex{},
		columns:      o.cols,
		hasOrderBy:   o.hasOrderBy,
	}, nil
}

// 初始化堆和map，保证至少有一个排序列相同的所有数据全部拿出。第一个返回值表示results还有没有值
func (o *DistinctMerger) initMapAndHeap(results []*sql.Rows, t *mapx.TreeMap[key, struct{}], h *heap2.Heap) error {

	// 初始化将所有sql.Rows的第一个元素塞进heap中
	for i := 0; i < len(results); i++ {
		if results[i].Next() {

			n, err := newDistinctNode(results[i], o.sortCols, i, o.hasOrderBy)
			if err != nil {
				return err
			}
			heap.Push(h, n)
		} else if results[i].Err() != nil {
			return results[i].Err()
		}
	}
	// 如果四个results里面的元素均为空表示没有已经没有数据了

	_, err := balance(results, t, o.sortCols, h, o.hasOrderBy)
	return err
}

func newDistinctNode(rows *sql.Rows, sortCols merger.SortColumns, index int, hasOrderBy bool) (*heap2.Node, error) {
	n, err := newNode(rows, sortCols, index)
	if err != nil {
		return nil, err
	}
	if !hasOrderBy {
		n.SortCols = []any{1}
	}
	return n, nil
}

func newNode(row rows.Rows, sortCols merger.SortColumns, index int) (*heap2.Node, error) {
	colsInfo, err := row.ColumnTypes()
	fmt.Printf("row err = %#v\n", err)
	if err != nil {
		return nil, err
	}
	columns := make([]any, 0, len(colsInfo))
	sortColumns := make([]any, sortCols.Len())
	for _, colInfo := range colsInfo {
		colName := colInfo.Name()
		colType := colInfo.ScanType()
		for colType.Kind() == reflect.Ptr {
			colType = colType.Elem()
		}
		log.Printf("colName = %s, colType = %s\n", colName, colType.String())
		column := reflect.New(colType).Interface()
		if sortCols.Has(colName) {
			log.Printf("sortCols = %#v, colName = %s, colType = %s\n", sortCols, colName, colType.String())
			sortIndex := sortCols.Find(colName)
			sortColumns[sortIndex] = column
		}
		columns = append(columns, column)
	}
	err = row.Scan(columns...)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(sortColumns); i++ {
		sortColumns[i] = reflect.ValueOf(sortColumns[i]).Elem().Interface()
	}
	for i := 0; i < len(columns); i++ {
		columns[i] = reflect.ValueOf(columns[i]).Elem().Interface()
	}
	log.Printf("sortColumns = %#v, columns = %#v\n", sortColumns, columns)
	return &heap2.Node{
		Index:    index,
		SortCols: sortColumns,
		Columns:  columns,
	}, nil
}

// 从heap中取出一个排序列的所有行，保存进treemap中
func balance(results []*sql.Rows, t *mapx.TreeMap[key, struct{}], sortCols merger.SortColumns, h *heap2.Heap, hasOrderBy bool) (bool, error) {
	var sortCol []any
	if h.Len() == 0 {
		return false, nil
	}
	for i := 0; ; i++ {
		if h.Len() == 0 {
			return false, nil
		}
		val := heap.Pop(h).(*heap2.Node)
		if i == 0 {
			sortCol = val.SortCols
		}
		// 相同元素进入treemap
		if compareKey(key{val.SortCols}, key{sortCol}) == 0 {
			err := t.Put(key{val.Columns}, struct{}{})
			if err != nil {
				return false, err
			}
			// 将后续元素加入heap
			r := results[val.Index]
			if r.Next() {
				n, err := newDistinctNode(r, sortCols, val.Index, hasOrderBy)
				if err != nil {
					return false, err
				}
				heap.Push(h, n)
			} else if r.Err() != nil {
				return false, r.Err()
			}
		} else {
			// 如果排序列不相同将 拿出来的元素，重新塞进heap中
			heap.Push(h, val)
			return true, nil
		}

	}
}

type DistinctRows struct {
	distinctCols []merger.ColumnInfo
	rowsList     []*sql.Rows
	sortCols     merger.SortColumns
	hp           *heap2.Heap
	mu           *sync.RWMutex
	treeMap      *mapx.TreeMap[key, struct{}]
	cur          []any
	closed       bool
	lastErr      error
	columns      []string
	hasOrderBy   bool
}

func (r *DistinctRows) NextResultSet() bool {
	// TODO implement me
	panic("implement me")
}

func (r *DistinctRows) ColumnTypes() ([]*sql.ColumnType, error) {
	// TODO implement me
	panic("implement me")
}

func (r *DistinctRows) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if r.hp.Len() == 0 && len(r.treeMap.Keys()) == 0 || r.lastErr != nil {
		_ = r.close()
		return false
	}
	val := r.treeMap.Keys()[0]
	r.cur = val.data
	// 删除当前的数据行
	_, _ = r.treeMap.Delete(val)
	// 当一个排序列的数据取完就取下一个排序列的全部数据
	if len(r.treeMap.Keys()) == 0 {
		_, err := balance(r.rowsList, r.treeMap, r.sortCols, r.hp, r.hasOrderBy)
		if err != nil {
			r.lastErr = err
			_ = r.close()
			return false
		}
	}
	return true
}

func (r *DistinctRows) Scan(dest ...any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastErr != nil {
		return r.lastErr
	}
	if r.closed {
		return errs.ErrMergerRowsClosed
	}
	if r.cur == nil {
		return errs.ErrMergerScanNotNext
	}
	var err error
	for i := 0; i < len(dest); i++ {
		err = rows.ConvertAssign(dest[i], r.cur[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *DistinctRows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.close()
}

func (r *DistinctRows) close() error {
	r.closed = true
	errorList := make([]error, 0, len(r.rowsList))
	for i := 0; i < len(r.rowsList); i++ {
		row := r.rowsList[i]
		err := row.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return multierr.Combine(errorList...)
}

func (r *DistinctRows) Columns() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return r.columns, nil
}

func (r *DistinctRows) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}
