package seacle

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"strings"
	"text/template"

	"github.com/serenize/snaker"
	"golang.org/x/tools/imports"
)

type columnInfo struct {
	Field  string
	Column string
	Type   string
}

type Generator struct {
	Tag string
}

func (g Generator) analyzeColumn(field reflect.StructField) (string, bool, bool) {
	// at first, find column from tag
	structTag := field.Tag
	tag, _ := structTag.Lookup(g.Tag)

	// if tag is empty, use snake case of Name
	if tag == "" {
		tag = snaker.CamelToSnake(field.Name)
	}

	// check flags ( `db:"id,primary"` means primary column, `db:"id,auto_increment" means auto increment column` )
	ss := strings.Split(tag, ",")
	isPrimary := false
	isAutoIncrement := false
	if len(ss) > 1 {
		for _, v := range ss[1:] {
			if v == "primary" {
				isPrimary = true
			}
			if v == "auto_increment" {
				isAutoIncrement = true
			}
		}
	}

	if ss[0] == "-" {
		// skip tag
		return "", false, false
	}

	return ss[0], isPrimary, isAutoIncrement
}

func (g Generator) analyzeStruct(tp reflect.Type) (primary []columnInfo, values []columnInfo, autoIncrementCol string) {
	for i := 0; i < tp.NumField(); i++ {
		f := tp.Field(i)
		p, v, auto := g.analyzeField(f)

		primary = append(primary, p...)
		values = append(values, v...)
		if autoIncrementCol == "" && auto != "" {
			autoIncrementCol = auto
		}
	}
	return
}

func (g Generator) analyzeField(field reflect.StructField) (primary []columnInfo, values []columnInfo, autoIncrementCol string) {
	tp := field.Type
	tag, ok := field.Tag.Lookup(g.Tag)
	if tag == "-" {
		return
	}
	if !ok {
		if tp.Kind() == reflect.Ptr {
			tp = tp.Elem()
		}
		if tp.Kind() == reflect.Struct {
			// recursive!
			p, v, auto := g.analyzeStruct(tp)
			primary = append(primary, p...)
			values = append(values, v...)
			if autoIncrementCol == "" && auto != "" {
				autoIncrementCol = auto
			}
			return
		}
	}

	column, isPrimary, isAutoIncrement := g.analyzeColumn(field)
	//log.Println("column=", column, "isPrimary=", isPrimary, "isAutoIncrement=", isAutoIncrement)
	if column == "" {
		return
	}
	colinfo := columnInfo{
		Field:  field.Name,
		Column: column,
		Type:   field.Type.String(),
	}

	if isPrimary {
		primary = append(primary, colinfo)
	} else {
		values = append(values, colinfo)
	}

	if isAutoIncrement {
		autoIncrementCol = colinfo.Column
	}

	return
}

func (g Generator) analyzeType(tp reflect.Type, pkg, table string) (map[string]interface{}, error) {
	if tp.Kind() == reflect.Ptr {
		tp = reflect.PtrTo(tp)
	}
	if tp.Kind() != reflect.Struct {
		return nil, fmt.Errorf("unexpected Type: %s", tp.String())
	}
	// note: Now, tp is not pointer type but struct type

	// Field analysis
	primary, values, autoIncrementCol := g.analyzeStruct(tp)

	// if there's no primary, firstCol is primary
	if len(primary) == 0 {
		if len(values) != 0 {
			log.Println("There's no primary columns. use first column as primary column:", values[0].Field)
			primary = append(primary, values[0])
			values = values[1:]
		}
	}

	vars := map[string]interface{}{
		"Package":       pkg,
		"Table":         table,
		"Typename":      tp.Name(),
		"Primary":       primary,
		"Values":        values,
		"AutoIncrement": autoIncrementCol,
		"AllColumns":    append(primary, values...),
	}

	return vars, nil
}

func (g Generator) Generate(tp reflect.Type, pkg, table, destfile string) error {
	vars, err := g.analyzeType(tp, pkg, table)
	if err != nil {
		return err
	}

	tmpl := template.Must(template.New("template.go").Parse(tx))
	buf := &bytes.Buffer{}
	err = tmpl.Execute(buf, vars)
	if err != nil {
		log.Println("failed to execute template:", err)
		return fmt.Errorf("failed to execute template: %s", err)
	}

	out, err := imports.Process(destfile, buf.Bytes(), &imports.Options{
		Comments: true,
		TabWidth: 4,
	})
	if err != nil {
		log.Printf("failed to goimports: err=%s", err)
		return fmt.Errorf("failed to goimports: err=%s", err)
	}

	err = ioutil.WriteFile(destfile, out, 0666)
	if err != nil {
		log.Printf("failed to create file %s: err=%s", destfile, err)
		return err
	}

	return nil
}

const tx = `// Code generated by seacle.Generator DO NOT EDIT
// About seacle: https://github.com/acidlemon/seacle
package {{ .Package }}

import (
	"database/sql"

	"github.com/acidlemon/seacle"
)

var _ seacle.Mappable = (*{{ .Typename }})(nil)

func (p *{{ .Typename }}) Table() string {
	return "{{ .Table }}"
}

func (p *{{ .Typename }}) Columns() []string {
	return []string{ {{ range $i, $v := .AllColumns }}"{{ $.Table }}.{{ $v.Column }}", {{ end }} }
}

func (p *{{ .Typename }}) PrimaryKeys() []string {
	return []string{ {{ range $i, $v := .Primary }}"{{ $v.Column }}", {{ end }} }
}

func (p *{{ .Typename }}) PrimaryValues() []interface{} {
	return []interface{}{ {{ range $i, $v := .Primary }}p.{{ $v.Field }}, {{ end }} }
}

func (p *{{ .Typename }}) ValueColumns() []string {
	return []string{ {{ range $i, $v := .Values }}"{{ $v.Column }}", {{ end }} }
}

func (p *{{ .Typename }}) Values() []interface{} {
	return []interface{}{ {{ range $i, $v := .Values }}p.{{ $v.Field }}, {{ end }} }
}

func (p *{{ .Typename }}) AutoIncrementColumn() string {
	return "{{ .AutoIncrement }}"
}

func (p *{{ .Typename }}) Scan(r seacle.RowScanner) error {
	{{ range $i, $v := .AllColumns }}var arg{{ $i }} {{ $v.Type }}
	{{ end }}
	err := r.Scan({{ range $i, $v := .AllColumns }}&arg{{ $i }}, {{ end }})
	if err == sql.ErrNoRows {
		return err
	} else if err != nil {
		// something wrong
		return err
	}

	{{ range $i, $v := .AllColumns }}p.{{ $v.Field }} = arg{{ $i }}
	{{ end }}
	return nil
}
`
