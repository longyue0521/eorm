package merger

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	testcases := []struct {
		name    string
		values  []any
		order   Order
		wantVal int
		kind    reflect.Kind
	}{
		{
			name:    "int8 ASC 1,2",
			values:  []any{int8(1), int8(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 DESC 1,2",
			values:  []any{int8(1), int8(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 ASC 2,1",
			values:  []any{int8(2), int8(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 DESC 2,1",
			values:  []any{int8(2), int8(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 equal",
			values:  []any{int8(2), int8(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Int8,
		},
		{
			name:    "int16 ASC 1,2",
			values:  []any{int16(1), int16(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 DESC 1,2",
			values:  []any{int16(1), int16(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 ASC 2,1",
			values:  []any{int16(2), int16(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 DESC 2,1",
			values:  []any{int16(2), int16(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 equal",
			values:  []any{int16(2), int16(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Int16,
		},
		{
			name:    "int32 ASC 1,2",
			values:  []any{int32(1), int32(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 DESC 1,2",
			values:  []any{int32(1), int32(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 ASC 2,1",
			values:  []any{int32(2), int32(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 DESC 2,1",
			values:  []any{int32(2), int32(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 equal",
			values:  []any{int32(2), int32(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Int32,
		},
		{
			name:    "int64 ASC 1,2",
			values:  []any{int64(1), int64(02)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 DESC 1,2",
			values:  []any{int64(1), int64(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 ASC 2,1",
			values:  []any{int64(2), int64(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 DESC 2,1",
			values:  []any{int64(2), int64(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 equal",
			values:  []any{int64(2), int64(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Int64,
		},
		{
			name:    "uint8 ASC 1,2",
			values:  []any{uint8(1), uint8(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 DESC 1,2",
			values:  []any{uint8(1), uint8(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 ASC 2,1",
			values:  []any{uint8(2), uint8(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 DESC 2,1",
			values:  []any{uint8(2), uint8(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 equal",
			values:  []any{uint8(2), uint8(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Uint8,
		},

		{
			name:    "uint16 ASC 1,2",
			values:  []any{uint16(1), uint16(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 DESC 1,2",
			values:  []any{uint16(1), uint16(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 ASC 2,1",
			values:  []any{uint16(2), uint16(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 DESC 2,1",
			values:  []any{uint16(2), uint16(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 equal",
			values:  []any{uint16(2), uint16(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint32 ASC 1,2",
			values:  []any{uint32(1), uint32(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 DESC 1,2",
			values:  []any{uint32(1), uint32(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 ASC 2,1",
			values:  []any{uint32(2), uint32(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 DESC 2,1",
			values:  []any{uint32(2), uint32(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 equal",
			values:  []any{uint32(2), uint32(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint64 ASC 1,2",
			values:  []any{uint64(1), uint64(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 DESC 1,2",
			values:  []any{uint64(1), uint64(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 ASC 2,1",
			values:  []any{uint64(2), uint64(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 DESC 2,1",
			values:  []any{uint64(2), uint64(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 equal",
			values:  []any{uint64(2), uint64(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Uint64,
		},
		{
			name:    "float32 ASC 1,2",
			values:  []any{float32(1.1), float32(2.1)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 DESC 1,2",
			values:  []any{float32(1.1), float32(2.1)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 ASC 2,1",
			values:  []any{float32(2), float32(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 DESC 2,1",
			values:  []any{float32(2.1), float32(1.1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 equal",
			values:  []any{float32(2.1), float32(2.1)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Float32,
		},
		{
			name:    "float64 ASC 1,2",
			values:  []any{1.1, 2.1},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 DESC 1,2",
			values:  []any{float64(1), float64(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 ASC 2,1",
			values:  []any{float64(2), float64(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 DESC 2,1",
			values:  []any{2.1, 1.1},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 equal",
			values:  []any{2.1, 2.1},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Float64,
		},
		{
			name:    "string equal",
			values:  []any{"x", "x"},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.String,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cmp, ok := CompareFuncMapping[tc.kind]
			require.True(t, ok)
			val := cmp(tc.values[0], tc.values[1], tc.order)
			assert.Equal(t, tc.wantVal, val)
		})
	}
}

func TestSortColumns(t *testing.T) {

	t.Run("零值", func(t *testing.T) {
		s := SortColumns{}
		require.True(t, s.IsZeroValue())
	})
}
