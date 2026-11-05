package turboseeder

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Formatter converts a value into something the database driver understands.
// It returns ok=false to let the next formatter (or the built-in rules) run.
type Formatter func(v any) (out any, ok bool)

var (
	formattersMu sync.Mutex
	formatters   atomic.Pointer[[]Formatter] // read lock-free on every value
)

// RegisterFormatter adds a custom value formatter, consulted before the
// built-in rules. Equivalent to ValueFormatter::extend() in the PHP package.
func RegisterFormatter(f Formatter) {
	formattersMu.Lock()
	defer formattersMu.Unlock()
	var next []Formatter
	if cur := formatters.Load(); cur != nil {
		next = append(next, *cur...)
	}
	next = append(next, f)
	formatters.Store(&next)
}

var (
	valuerType = reflect.TypeFor[driver.Valuer]()
	timeType   = reflect.TypeFor[time.Time]()
)

// Normalize converts a generated value into a driver-friendly value:
//
//   - nil and nil pointers become NULL; other pointers are dereferenced
//   - driver.Valuer, time.Time, []byte, string, bool and numbers pass through
//   - named types (type Status string) are converted to their base type
//   - json.RawMessage becomes a string
//   - maps, slices, arrays and structs are JSON-encoded
func Normalize(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if fs := formatters.Load(); fs != nil {
		for _, f := range *fs {
			if out, ok := f(v); ok {
				v = out
				break
			}
		}
	}
	switch x := v.(type) {
	case nil:
		return nil, nil
	case string, []byte, bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64, time.Time:
		return x, nil
	case json.RawMessage:
		return string(x), nil
	case driver.Valuer:
		rv := reflect.ValueOf(x)
		if rv.Kind() == reflect.Pointer && rv.IsNil() {
			return nil, nil
		}
		return x, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return nil, nil
		}
		return Normalize(rv.Elem().Interface())
	case reflect.String:
		return rv.String(), nil
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return rv.Bytes(), nil
		}
		if rv.IsNil() {
			return nil, nil
		}
		fallthrough
	case reflect.Map, reflect.Array, reflect.Struct:
		if rv.Kind() == reflect.Map && rv.IsNil() {
			return nil, nil
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("turboseeder: encode %T as JSON: %w", v, err)
		}
		return string(b), nil
	}
	return nil, fmt.Errorf("turboseeder: unsupported value type %T", v)
}

// TextValue renders a normalized value as text for bulk-load formats
// (CSV, COPY). null reports SQL NULL. Loaders use this so every strategy
// formats values the same way.
func TextValue(v any, d Dialect) (s string, null bool, err error) {
	if vr, ok := v.(driver.Valuer); ok {
		if v, err = vr.Value(); err != nil {
			return "", false, err
		}
	}
	switch x := v.(type) {
	case nil:
		return "", true, nil
	case string:
		return x, false, nil
	case []byte:
		if d == Postgres {
			return `\x` + hex.EncodeToString(x), false, nil
		}
		return string(x), false, nil
	case bool:
		if x {
			return "1", false, nil
		}
		return "0", false, nil
	case int:
		return strconv.Itoa(x), false, nil
	case int64:
		return strconv.FormatInt(x, 10), false, nil
	case uint64:
		return strconv.FormatUint(x, 10), false, nil
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), false, nil
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32), false, nil
	case time.Time:
		if d == Postgres {
			return x.Format("2006-01-02 15:04:05.999999Z07:00"), false, nil
		}
		// go-sql-driver/mysql sends times in UTC by default (loc=UTC).
		return x.UTC().Format("2006-01-02 15:04:05.999999"), false, nil
	}
	// Remaining sized ints and anything the Valuer produced.
	n, err := Normalize(v)
	if err != nil {
		return "", false, err
	}
	switch x := n.(type) {
	case int64, uint64, string, []byte, nil, bool, float64, time.Time:
		return TextValue(x, d)
	case int8, int16, int32, uint, uint8, uint16, uint32:
		return fmt.Sprint(x), false, nil
	}
	return fmt.Sprint(n), false, nil
}
