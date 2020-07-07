package seacle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
)

type Context = context.Context

func expandPlaceholder(q string, args ...interface{}) (string, []interface{}) {
	if len(args) == 0 {
		return q, args
	}

	re := regexp.MustCompile("\\?")
	exargs := []interface{}{}
	count := 0
	q = re.ReplaceAllStringFunc(q, func(match string) string {
		if count >= len(args) {
			return match // do nothing
		}
		tp := reflect.TypeOf(args[count])
		val := args[count]
		count++
		if tp.Kind() == reflect.Slice {
			vp := reflect.ValueOf(val)
			for i := 0; i < vp.Len(); i++ {
				exargs = append(exargs, vp.Index(i).Interface())
			}
			return strings.TrimSuffix(strings.Repeat("?,", vp.Len()), ",")
		} else {
			exargs = append(exargs, val)
			return match
		}
	})

	return q, exargs
}

type Selectable interface {
	QueryContext(ctx Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx Context, query string, args ...interface{}) *sql.Row
}

func Select(ctx Context, s Selectable, out interface{}, fragment string, args ...interface{}) error {
	// check about "out"
	var tp reflect.Type
	isVal := false
	{
		checkTp := reflect.TypeOf(out)
		typeName := checkTp.String()
		if checkTp.Kind() != reflect.Ptr {
			return fmt.Errorf("out is not pointer: %s", typeName)
		}

		checkTp = checkTp.Elem()
		if checkTp.Kind() != reflect.Slice {
			return fmt.Errorf("out is not pointer of slice: %s", typeName)
		}

		checkTp = checkTp.Elem()
		it := reflect.TypeOf((*Mappable)(nil)).Elem()
		if !checkTp.Implements(it) {
			ptrTp := reflect.PtrTo(checkTp)
			if ptrTp.Implements(it) {
				tp = ptrTp
				isVal = true
			} else {
				return fmt.Errorf("out is not pointer of slice of Mappable: %s", typeName)
			}
		} else {
			tp = checkTp
		}
	}

	columns, table, err := if2select(tp)
	if err != nil {
		return fmt.Errorf("invalid output container: %s", err.Error())
	}

	query := fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(columns, ", "), table, fragment)
	q, exargs := expandPlaceholder(query, args...)
	rows, err := s.QueryContext(ctx, q, exargs...)
	if err != nil {
		if err == sql.ErrNoRows {
			return err
		}
		msg := fmt.Sprintf("Select: QueryContext returned error: %s", err)
		log.Println(msg)
		return errors.New(msg)
	}
	defer rows.Close()

	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	outSliceVp := reflect.Indirect(reflect.ValueOf(out))
	for rows.Next() {
		vp := reflect.New(tp)
		mappable := vp.Interface().(Mappable)
		err := mappable.Scan(rows)
		if err != nil {
			return err
		}

		if isVal {
			outSliceVp.Set(reflect.Append(outSliceVp, reflect.Indirect(vp)))
		} else {
			outSliceVp.Set(reflect.Append(outSliceVp, vp))
		}
	}
	return nil
}

func SelectRow(ctx Context, s Selectable, out interface{}, fragment string, args ...interface{}) error {
	// check about "out"
	tp := reflect.TypeOf(out)
	it := reflect.TypeOf((*Mappable)(nil)).Elem()
	if !tp.Implements(it) {
		return fmt.Errorf("out is not Mappable: %s", tp.String())
	}

	columns, table, err := if2select(tp)
	if err != nil {
		return fmt.Errorf("invalid output container: %s", err.Error())
	}

	query := fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(columns, ", "), table, fragment)
	q, exargs := expandPlaceholder(query, args...)
	row := s.QueryRowContext(ctx, q, exargs...)
	mappable := reflect.ValueOf(out).Interface().(Mappable)
	err = mappable.Scan(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return err
		}
		msg := fmt.Sprintf("Select: QueryContext returned error: %s", err)
		log.Println(msg)
		return errors.New(msg)
	}

	return nil
}

type RowScanner interface {
	Scan(dest ...interface{}) error
}

type Mappable interface {
	Table() string
	Columns() []string
	Scan(r RowScanner) error
}

func if2select(mappableTp reflect.Type) ([]string, string, error) {
	vp := reflect.Zero(mappableTp)
	tableMethod := vp.MethodByName("Table")
	columnsMethod := vp.MethodByName("Columns")

	tableValue := tableMethod.Call(nil)
	columnsValue := columnsMethod.Call(nil)

	cols := columnsValue[0].Interface()

	return cols.([]string), tableValue[0].String(), nil
}
