package turboseeder

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

// FromModel seeds from a struct "factory": fn returns one model per index.
// It is the Go counterpart of fromFactory().
//
// The table is taken from a TableName() string method (the GORM convention),
// otherwise the snake_case plural of the type name ("BlogPost" -> "blog_posts").
// Columns come from exported fields: the `db:"name"` tag, then
// `gorm:"column:name"`, then the snake_case field name. Embedded structs are
// flattened, `db:"-"` skips a field, and relation-looking fields (structs,
// slices of structs) without an explicit tag are skipped. Auto-increment,
// identity and generated columns are dropped after schema introspection.
func FromModel[T any](db *sql.DB, fn func(i int) T) *Seeder {
	var zero T
	t := reflect.TypeOf(zero)
	s := ForTable(db, "")
	if t == nil || t.Kind() != reflect.Struct {
		return s.fail("FromModel needs a struct type, got %v", t)
	}
	s.table = modelTable(zero, t)
	fields := modelFields(t)
	if len(fields) == 0 {
		return s.fail("FromModel: %s has no mappable fields", t)
	}
	s.gen = &modelGen[T]{fn: fn, fields: fields}
	return s
}

type modelField struct {
	column string
	index  []int
}

type modelGen[T any] struct {
	fn     func(i int) T
	fields []modelField
}

func (g *modelGen[T]) columns() []string {
	cols := make([]string, len(g.fields))
	for i, f := range g.fields {
		cols[i] = f.column
	}
	return cols
}

func (g *modelGen[T]) bind(cols []string) (rowFunc, error) {
	byCol := make(map[string][]int, len(g.fields))
	for _, f := range g.fields {
		byCol[f.column] = f.index
	}
	idx := make([][]int, len(cols))
	for i, c := range cols {
		fi, ok := byCol[c]
		if !ok {
			return nil, fmt.Errorf("turboseeder: model has no field for column %q", c)
		}
		idx[i] = fi
	}
	return func(i int, dst []any) error {
		m := g.fn(i)
		v := reflect.ValueOf(&m).Elem()
		for c, fi := range idx {
			f, err := v.FieldByIndexErr(fi)
			if err != nil { // nil embedded pointer
				dst[c] = nil
				continue
			}
			dst[c] = f.Interface()
		}
		return nil
	}, nil
}

type tabler interface{ TableName() string }

func modelTable(zero any, t reflect.Type) string {
	if tn, ok := zero.(tabler); ok {
		return tn.TableName()
	}
	if tn, ok := reflect.New(t).Interface().(tabler); ok {
		return tn.TableName()
	}
	return pluralize(snakeCase(t.Name()))
}

func modelFields(t reflect.Type) []modelField {
	var out []modelField
	seen := map[string]bool{}
	for _, f := range reflect.VisibleFields(t) {
		if !f.IsExported() {
			continue
		}
		col, explicit, skip := fieldColumn(f)
		if skip {
			continue
		}
		if f.Anonymous && derefType(f.Type).Kind() == reflect.Struct && !explicit && !isScalarStruct(f.Type) {
			continue // embedded struct: its promoted fields are visited separately
		}
		if !explicit && looksLikeRelation(f.Type) {
			continue
		}
		if seen[col] {
			continue
		}
		seen[col] = true
		out = append(out, modelField{column: col, index: f.Index})
	}
	return out
}

func fieldColumn(f reflect.StructField) (col string, explicit, skip bool) {
	if tag, ok := f.Tag.Lookup("db"); ok {
		name := strings.Split(tag, ",")[0]
		if name == "-" {
			return "", false, true
		}
		if name != "" {
			return name, true, false
		}
	}
	if tag, ok := f.Tag.Lookup("gorm"); ok {
		for _, part := range strings.Split(tag, ";") {
			part = strings.TrimSpace(part)
			if part == "-" || part == "-:all" || part == "-:migration" {
				return "", false, true
			}
			if name, ok := strings.CutPrefix(part, "column:"); ok {
				return name, true, false
			}
		}
	}
	return snakeCase(f.Name), false, false
}

func derefType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// isScalarStruct reports struct types that map to a single column.
func isScalarStruct(t reflect.Type) bool {
	return derefType(t) == timeType || t.Implements(valuerType) || reflect.PointerTo(derefType(t)).Implements(valuerType)
}

func looksLikeRelation(t reflect.Type) bool {
	if isScalarStruct(t) {
		return false
	}
	switch d := derefType(t); d.Kind() {
	case reflect.Struct:
		return true
	case reflect.Slice, reflect.Array:
		e := derefType(d.Elem())
		return e.Kind() == reflect.Struct && !isScalarStruct(d.Elem())
	}
	return false
}

func snakeCase(s string) string {
	var b strings.Builder
	r := []rune(s)
	for i, c := range r {
		if unicode.IsUpper(c) {
			// Insert "_" at word boundaries: aB -> a_b, ABc -> a_bc (UserID -> user_id).
			if i > 0 && (unicode.IsLower(r[i-1]) || unicode.IsDigit(r[i-1]) ||
				(i+1 < len(r) && unicode.IsLower(r[i+1]) && unicode.IsUpper(r[i-1]))) {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(c))
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}

func pluralize(s string) string {
	switch {
	case s == "":
		return s
	case strings.HasSuffix(s, "y") && len(s) > 1 && !strings.ContainsRune("aeiou", rune(s[len(s)-2])):
		return s[:len(s)-1] + "ies"
	case strings.HasSuffix(s, "s"), strings.HasSuffix(s, "x"), strings.HasSuffix(s, "z"),
		strings.HasSuffix(s, "ch"), strings.HasSuffix(s, "sh"):
		return s + "es"
	}
	return s + "s"
}
