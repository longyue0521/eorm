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
	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	heap2 "github.com/ecodeclub/eorm/internal/merger/internal/sortmerger/heap"
	"github.com/ecodeclub/eorm/internal/rows"
	"go.uber.org/multierr"
)

type Merger struct {
	sortColumns merger.SortColumns
	preScanAll  bool
	columnInfos []merger.ColumnInfo
	columnNames []string
}

func NewDistinctMerger(distinctCols []merger.ColumnInfo, sortColumns merger.SortColumns) (*Merger, error) {

	if len(distinctCols) == 0 {
		return nil, errs.ErrDistinctColsIsNull
	}

	if sortColumns.IsZeroValue() {
		columns := slice.Map(distinctCols, func(idx int, src merger.ColumnInfo) merger.ColumnInfo {
			src.Order = merger.OrderASC
			return src
		})
		sortColumns, _ = merger.NewSortColumns(columns...)
		return &Merger{
			sortColumns: sortColumns,
			columnInfos: distinctCols,
			preScanAll:  true,
		}, nil
	}

	// 检查sortCols必须全在distinctCols
	distinctSet := make(map[string]struct{})
	for _, col := range distinctCols {
		name := col.SelectName()
		_, ok := distinctSet[name]
		if ok {
			return nil, errs.ErrDistinctColsRepeated
		} else {
			distinctSet[name] = struct{}{}
		}
		// 补充缺少的列
		if !sortColumns.Has(name) {
			col.Order = merger.OrderASC
			sortColumns.Add(col)
		}
	}
	for i := 0; i < sortColumns.Len(); i++ {
		val := sortColumns.Get(i)
		if _, ok := distinctSet[val.SelectName()]; !ok {
			return nil, errs.ErrSortColListNotContainDistinctCol
		}
	}
	return &Merger{
		sortColumns: sortColumns,
		columnInfos: distinctCols,
		preScanAll:  true,
		// preScanAll:  false,
	}, nil
}

func (m *Merger) Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error) {
	// 检测results是否符合条件
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}

	for i := 0; i < len(results); i++ {
		err := m.checkColumns(results[i])
		if err != nil {
			return nil, err
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return m.initRows(results)
}
func (m *Merger) checkColumns(rows rows.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	// 判断数据库里的列只有去重列，且顺序要和定义的顺序一致
	if len(cols) != len(m.columnInfos) {
		return errs.ErrDistinctColsNotInCols
	}
	fmt.Printf("columns = %#v\n", m.columnInfos)
	for _, distinctColumn := range m.columnInfos {
		if cols[distinctColumn.Index] != distinctColumn.SelectName() {
			return errs.ErrDistinctColsNotInCols
		}
	}
	m.columnNames = cols
	return err
}

func (m *Merger) initRows(results []rows.Rows) (*Rows, error) {
	r := &Rows{
		columnInfos: m.columnInfos,
		rowsList:    results,
		sortColumns: m.sortColumns,
		mu:          &sync.RWMutex{},
		columnNames: m.columnNames,
		preScanAll:  m.preScanAll,
	}
	r.hp = heap2.NewHeap(make([]*heap2.Node, 0, len(results)), m.sortColumns)

	t, err := mapx.NewTreeMap[treeMapKey, struct{}](func(src treeMapKey, dst treeMapKey) int {
		return src.compare(dst)
	})
	if err != nil {
		return nil, err
	}
	r.treeMap = t

	// 下方init会把rowsList中所有数据扫描到内存然后关闭其中所有rows.Rows,所以要提前缓存住列类型信息
	columnTypes, err := r.rowsList[0].ColumnTypes()
	if err != nil {
		return nil, err
	}
	r.columnTypes = columnTypes

	err = r.init()
	if err != nil {
		return nil, err
	}
	return r, nil
}

type treeMapKey struct {
	sortValues  []any
	values      []any
	sortColumns merger.SortColumns
}

// func (k treeMapKey) isZeroValue() bool {
// 	return k.sortValues == nil && k.sortColumns.IsZeroValue()
// }

func (k treeMapKey) compare(b treeMapKey) int {
	keyLen := len(k.sortValues)
	for i := 0; i < keyLen; i++ {
		var cmp func(any, any, merger.Order) int
		if _, ok := k.sortValues[i].(driver.Valuer); ok {
			cmp = merger.CompareNullable
		} else {
			kind := reflect.TypeOf(k.sortValues[i]).Kind()
			cmp = merger.CompareFuncMapping[kind]
		}
		res := cmp(k.sortValues[i], b.sortValues[i], k.sortColumns.Get(i).Order)
		if res != 0 {
			return res
		}
	}
	return 0
}

// func compareKey(a, b treeMapKey) int {
// 	keyLen := len(a.data)
// 	for i := 0; i < keyLen; i++ {
// 		var cmp func(any, any, merger.Order) int
// 		if _, ok := a.data[i].(driver.Valuer); ok {
// 			cmp = merger.CompareNullable
// 		} else {
// 			cmp = merger.CompareFuncMapping[reflect.TypeOf(a.data[i]).Kind()]
// 		}
// 		res := cmp(a.data[i], b.data[i], merger.OrderASC)
// 		if res != 0 {
// 			return res
// 		}
// 	}
// 	return 0
// }

// // 初始化堆和map，保证至少有一个排序列相同的所有数据全部拿出。第一个返回值表示results还有没有值
// func (o *Merger) initMapAndHeap(results []rows.Rows, t *mapx.TreeMap[treeMapKey, struct{}], h *heap2.Heap) error {
//
// 	// 初始化将所有sql.Rows的第一个元素塞进heap中
// 	for i := 0; i < len(results); i++ {
// 		if results[i].Next() {
//
// 			n, err := newDistinctNode(results[i], o.sortCols, i, o.hasOrderBy)
// 			if err != nil {
// 				return err
// 			}
// 			heap.Push(h, n)
// 		} else if results[i].Err() != nil {
// 			return results[i].Err()
// 		}
// 	}
// 	// 如果四个results里面的元素均为空表示没有已经没有数据了
//
// 	_, err := deduplicate(results, t, o.sortCols, h, o.hasOrderBy)
// 	return err
// }
//
// func newDistinctNode(rows rows.Rows, sortCols merger.SortColumns, index int, hasOrderBy bool) (*heap2.Node, error) {
// 	n, err := newNode(rows, sortCols, index)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if !hasOrderBy {
// 		n.SortColumnValues = []any{1}
// 	}
// 	return n, nil
// }
//
// func newNode(row rows.Rows, sortCols merger.SortColumns, index int) (*heap2.Node, error) {
// 	colsInfo, err := row.ColumnTypes()
// 	fmt.Printf("row err = %#v\n", err)
// 	if err != nil {
// 		return nil, err
// 	}
// 	columns := make([]any, 0, len(colsInfo))
// 	sortColumns := make([]any, sortCols.Len())
// 	log.Printf("sortColumns1 = %#v, len=%d\n", sortColumns, len(sortColumns))
// 	for _, colInfo := range colsInfo {
// 		colName := colInfo.Name()
// 		colType := colInfo.ScanType()
// 		for colType.Kind() == reflect.Ptr {
// 			colType = colType.Elem()
// 		}
// 		log.Printf("colName = %s, colType = %s\n", colName, colType.String())
// 		column := reflect.New(colType).Interface()
// 		if sortCols.Has(colName) {
// 			log.Printf("sortCols = %#v, colName = %s, colType = %s\n", sortCols, colName, colType.String())
// 			sortIndex := sortCols.Find(colName)
// 			sortColumns[sortIndex] = column
// 		}
// 		columns = append(columns, column)
// 	}
// 	err = row.Scan(columns...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	log.Printf("sortColumns2 = %#v, len = %d\n", sortColumns, len(sortColumns))
// 	for i := 0; i < len(sortColumns); i++ {
// 		v := reflect.ValueOf(sortColumns[i])
// 		if v.IsValid() && !v.IsZero() {
// 			sortColumns[i] = v.Elem().Interface()
// 		}
// 	}
// 	for i := 0; i < len(columns); i++ {
// 		columns[i] = reflect.ValueOf(columns[i]).Elem().Interface()
// 	}
// 	log.Printf("sortColumns = %#v, columns = %#v\n", sortColumns, columns)
// 	return &heap2.Node{
// 		Index:            index,
// 		SortColumnValues: sortColumns,
// 		ColumnValues:     columns,
// 	}, nil
// }
//
// // 从heap中取出一个排序列的所有行，保存进treemap中
// func deduplicate(results []rows.Rows, t *mapx.TreeMap[treeMapKey, struct{}], sortCols merger.SortColumns, h *heap2.Heap, hasOrderBy bool) (bool, error) {
// 	var sortCol []any
// 	if h.Len() == 0 {
// 		return false, nil
// 	}
// 	for i := 0; ; i++ {
// 		if h.Len() == 0 {
// 			return false, nil
// 		}
// 		val := heap.Pop(h).(*heap2.Node)
// 		if i == 0 {
// 			sortCol = val.SortColumnValues
// 		}
// 		// 相同元素进入treemap
// 		if compareKey(treeMapKey{val.SortColumnValues}, treeMapKey{sortCol}) == 0 {
// 			err := t.Put(treeMapKey{val.ColumnValues}, struct{}{})
// 			if err != nil {
// 				return false, err
// 			}
// 			// 将后续元素加入heap
// 			rr := results[val.Index]
// 			if rr.Next() {
// 				n, err := newDistinctNode(rr, sortCols, val.Index, hasOrderBy)
// 				if err != nil {
// 					return false, err
// 				}
// 				heap.Push(h, n)
// 			} else if rr.Err() != nil {
// 				return false, rr.Err()
// 			}
// 		} else {
// 			// 如果排序列不相同将 拿出来的元素，重新塞进heap中
// 			heap.Push(h, val)
// 			return true, nil
// 		}
//
// 	}
// }

// 初始化堆和map，保证至少有一个排序列相同的所有数据全部拿出。第一个返回值表示results还有没有值
func (r *Rows) init() error {
	// 初始化将所有sql.Rows的第一个元素塞进heap中
	err := r.scanRowsIntoHeap()
	if err != nil {
		return err
	}
	// 如果四个results里面的元素均为空表示没有已经没有数据了
	_, err = r.deduplicate()
	return err
}

func (r *Rows) scanRowsIntoHeap() error {
	var err error
	for i := 0; i < len(r.rowsList); i++ {
		if r.preScanAll {
			err = r.preScanAllRows(i)

		} else {
			err = r.preScanOneRows(i)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Rows) preScanAllRows(idx int) error {
	for r.rowsList[idx].Next() {
		n, err := r.newHeapNode(r.rowsList[idx], idx)
		if err != nil {
			return err
		}
		heap.Push(r.hp, n)
	}
	if r.rowsList[idx].Err() != nil {
		return r.rowsList[idx].Err()
	}
	return nil
}

func (r *Rows) preScanOneRows(idx int) error {
	if r.rowsList[idx].Next() {
		n, err := r.newHeapNode(r.rowsList[idx], idx)
		if err != nil {
			return err
		}
		heap.Push(r.hp, n)
	} else if r.rowsList[idx].Err() != nil {
		return r.rowsList[idx].Err()
	}
	return nil
}

func (r *Rows) newHeapNode(row rows.Rows, index int) (*heap2.Node, error) {
	colsInfo, err := row.ColumnTypes()
	fmt.Printf("row err = %#v\n", err)
	if err != nil {
		return nil, err
	}
	columnValues := make([]any, 0, len(colsInfo))
	sortColumnValues := make([]any, r.sortColumns.Len())
	log.Printf("sortColumns1 = %#v, len=%d\n", sortColumnValues, len(sortColumnValues))
	for _, colInfo := range colsInfo {
		colName := colInfo.Name()
		colType := colInfo.ScanType()
		for colType.Kind() == reflect.Ptr {
			colType = colType.Elem()
		}
		log.Printf("colName = %s, colType = %s\n", colName, colType.String())
		column := reflect.New(colType).Interface()
		if r.sortColumns.Has(colName) {
			log.Printf("sortCols = %#v, colName = %s, colType = %s\n", r.sortColumns, colName, colType.String())
			sortIndex := r.sortColumns.Find(colName)
			sortColumnValues[sortIndex] = column
		}
		columnValues = append(columnValues, column)
	}
	err = row.Scan(columnValues...)
	if err != nil {
		return nil, err
	}
	log.Printf("sortColumns2 = %#v, len = %d\n", sortColumnValues, len(sortColumnValues))
	for i := 0; i < len(sortColumnValues); i++ {
		v := reflect.ValueOf(sortColumnValues[i])
		if v.IsValid() && !v.IsZero() {
			sortColumnValues[i] = v.Elem().Interface()
		}
	}
	for i := 0; i < len(columnValues); i++ {
		columnValues[i] = reflect.ValueOf(columnValues[i]).Elem().Interface()
	}
	log.Printf("sortColumns = %#v, columns = %#v\n", sortColumnValues, columnValues)
	node := &heap2.Node{
		RowsListIndex:    index,
		SortColumnValues: sortColumnValues,
		ColumnValues:     columnValues,
	}
	fmt.Printf("heap node = %#v, preScanAll = %v\n", node, r.preScanAll)
	// if !r.hasOrderBy {
	// 	node.SortColumnValues = []any{1}
	// }
	return node, nil
}

func (r *Rows) deduplicate() (bool, error) {
	// var prevKey treeMapKey
	// // var sortCol []any
	// if r.hp.Len() == 0 {
	// 	return false, nil
	// }
	// for {
	// 	if r.hp.Len() == 0 {
	// 		return false, nil
	// 	}
	// 	node := heap.Pop(r.hp).(*heap2.Node)
	// 	if prevKey.isZeroValue() {
	// 		prevKey = treeMapKey{data: node.SortColumnValues}
	// 	}
	//
	// 	// 相同元素进入treemap
	// 	key := treeMapKey{data: node.SortColumnValues}
	// 	if key.compare(prevKey) == 0 {
	// 		err := r.treeMap.Put(key, struct{}{})
	// 		if err != nil {
	// 			return false, err
	// 		}
	// 		// 将后续元素加入heap
	// 		err = r.preScanAllRows(node.RowsListIndex)
	// 		if err != nil {
	// 			return false, err
	// 		}
	// 	} else {
	// 		// 如果排序列不相同将 拿出来的元素，重新塞进heap中
	// 		heap.Push(r.hp, node)
	// 		return true, nil
	// 	}
	// }
	//
	for r.hp.Len() > 0 {
		node := heap.Pop(r.hp).(*heap2.Node)
		fmt.Printf("Pop nodes = %#v\n", node)

		key := treeMapKey{
			sortValues:  node.SortColumnValues,
			values:      node.ColumnValues,
			sortColumns: r.sortColumns,
		}
		err := r.treeMap.Put(key, struct{}{})
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

type Rows struct {
	columnInfos []merger.ColumnInfo
	rowsList    []rows.Rows
	columnTypes []*sql.ColumnType
	sortColumns merger.SortColumns
	hp          *heap2.Heap
	mu          *sync.RWMutex
	treeMap     *mapx.TreeMap[treeMapKey, struct{}]
	cur         []any
	closed      bool
	lastErr     error
	columnNames []string
	preScanAll  bool
}

func (r *Rows) NextResultSet() bool {
	return false
}

func (r *Rows) ColumnTypes() ([]*sql.ColumnType, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("%w", errs.ErrMergerRowsClosed)
	}
	return r.columnTypes, nil
}

func (r *Rows) Next() bool {
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
	r.cur = val.values
	// 删除当前的数据行
	_, _ = r.treeMap.Delete(val)

	if len(r.treeMap.Keys()) == 0 {
		// 当一个排序列的数据取完就取下一个排序列的全部数据
		_, err := r.deduplicate()
		if err != nil {
			r.lastErr = err
			_ = r.close()
			return false
		}
	}

	return true
}

func (r *Rows) Scan(dest ...any) error {
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

func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.close()
}

func (r *Rows) close() error {
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

func (r *Rows) Columns() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return r.columnNames, nil
}

func (r *Rows) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}
