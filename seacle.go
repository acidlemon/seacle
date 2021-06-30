package seacle

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

type Context = context.Context

var mappableIf = reflect.TypeOf((*Mappable)(nil)).Elem()

func expandPlaceholder(q string, args ...interface{}) (string, []interface{}) {
	if len(args) == 0 {
		return q, args
	}

	re := regexp.MustCompile("\\?")
	exargs := []interface{}{}
	count := 0
	query := re.ReplaceAllStringFunc(q, func(match string) string {
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

	return query, exargs
}

type Selectable interface {
	QueryContext(ctx Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx Context, query string, args ...interface{}) *sql.Row
}

func QueryContext(ctx Context, s Selectable, query string, args ...interface{}) (*sql.Rows, error) {
	query, exargs := expandPlaceholder(query, args...)
	return s.QueryContext(ctx, query, exargs...)
}
func QueryRowContext(ctx Context, s Selectable, query string, args ...interface{}) *sql.Row {
	query, exargs := expandPlaceholder(query, args...)
	return s.QueryRowContext(ctx, query, exargs...)
}

func Select(ctx Context, s Selectable, out interface{}, fragment string, args ...interface{}) error {
	// check about "out"
	var tp reflect.Type
	isVal := false
	{
		checkTp := reflect.TypeOf(out)
		typeName := checkTp.String()
		if checkTp.Kind() != reflect.Ptr {
			return fmt.Errorf("Select: out is not pointer: %s", typeName)
		}

		checkTp = checkTp.Elem()
		if checkTp.Kind() != reflect.Slice {
			return fmt.Errorf("Select: out is not pointer of slice: %s", typeName)
		}

		checkTp = checkTp.Elem()
		if !checkTp.Implements(mappableIf) {
			ptrTp := reflect.PtrTo(checkTp)
			if ptrTp.Implements(mappableIf) {
				tp = ptrTp
				isVal = true
			} else {
				return fmt.Errorf("Select: out is not pointer of slice of Mappable: %s", typeName)
			}
		} else {
			tp = checkTp
		}
	}

	columns, table, err := if2select(tp)
	if err != nil {
		return fmt.Errorf("Select: Invalid output container: %s", err.Error())
	}

	q := fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(columns, ", "), table, fragment)
	query, exargs := expandPlaceholder(q, args...)
	rows, err := s.QueryContext(ctx, query, exargs...)
	if err != nil {
		if err == sql.ErrNoRows {
			return err
		}
		return formatError("Select: QueryContext returned error", query, exargs, err)
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
	if !tp.Implements(mappableIf) {
		return fmt.Errorf("SelectRow: out is not Mappable: %s", tp.String())
	}

	columns, table, err := if2select(tp)
	if err != nil {
		return fmt.Errorf("SelectRow: Invalid output container: %s", err.Error())
	}

	q := fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(columns, ", "), table, fragment)
	query, exargs := expandPlaceholder(q, args...)
	row := s.QueryRowContext(ctx, query, exargs...)
	mappable := reflect.ValueOf(out).Interface().(Mappable)
	err = mappable.Scan(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return err
		}
		return formatError("SelectRow: QueryRowContext returned error", q, exargs, err)
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

type Executable interface {
	Selectable
	ExecContext(ctx Context, query string, args ...interface{}) (sql.Result, error)
}

type Modifiable interface {
	Table() string
	PrimaryKeys() []string
	PrimaryValues() []interface{}
	ValueColumns() []string
	Values() []interface{}
	AutoIncrementColumn() string
}

func Insert(ctx Context, e Executable, in Modifiable) (int64, error) {
	columns := in.PrimaryKeys()
	columns = append(columns, in.ValueColumns()...)
	args := in.PrimaryValues()
	args = append(args, in.Values()...)
	if in.AutoIncrementColumn() != "" {
		target := in.AutoIncrementColumn()
		tmpColumns := make([]string, 0, len(columns))
		tmpArgs := make([]interface{}, 0, len(args))
		for i, v := range columns {
			if v != target {
				tmpColumns = append(tmpColumns, columns[i])
				tmpArgs = append(tmpArgs, args[i])
			}
		}
		columns = tmpColumns
		args = tmpArgs
	}

	q := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (?)`, in.Table(), strings.Join(columns, ","))
	query, exargs := expandPlaceholder(q, args)

	result, err := e.ExecContext(ctx, query, exargs...)
	if err != nil {
		return 0, formatError("Insert: ExecContext returned error", query, exargs, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, formatError("Insert: Failed to get LastInsertId", query, exargs, err)
	}
	return id, err
}

// TODO
// func BulkInsert(ctx Context, e Executable, in []Modifiable) (int64, error) {
// }

func Update(ctx Context, e Executable, in Modifiable) error {
	pkey := in.PrimaryKeys()
	kv := []string{}
	for _, v := range pkey {
		kv = append(kv, fmt.Sprintf("%s = ?", v))
	}
	cond := strings.Join(kv, " AND ")

	cols := in.ValueColumns()
	kv = make([]string, 0)
	for _, v := range cols {
		kv = append(kv, fmt.Sprintf("%s = ?", v))
	}
	set := strings.Join(kv, ", ")

	query := fmt.Sprintf(`UPDATE %s SET %s WHERE %s`, in.Table(), set, cond)
	exargs := in.Values()
	exargs = append(exargs, in.PrimaryValues()...)

	_, err := e.ExecContext(ctx, query, exargs...)
	if err != nil {
		return formatError("Update: ExecContext returned error", query, exargs, err)
	}
	return nil
}

func Delete(ctx Context, e Executable, in Modifiable) error {
	pkey := in.PrimaryKeys()
	kv := []string{}
	for _, v := range pkey {
		kv = append(kv, fmt.Sprintf("%s = ?", v))
	}
	cond := strings.Join(kv, " AND ")

	query := fmt.Sprintf(`DELETE FROM %s WHERE %s`, in.Table(), cond)
	exargs := in.PrimaryValues()

	_, err := e.ExecContext(ctx, query, exargs...)
	if err != nil {
		return formatError("Delete: ExecContext returned error", query, exargs, err)
	}
	return nil
}

func formatError(message, query string, args []interface{}, err error) error {
	argsstr := make([]string, 0, len(args))
	for _, v := range args {
		argsstr = append(argsstr, fmt.Sprintf(`"%v"`, v))
	}

	return fmt.Errorf(`%s: err="%s", query="%s", args=%s`,
		message, err, query, "["+strings.Join(argsstr, ", ")+"]")
}
