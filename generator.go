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

func (g Generator) analyzeColumn(field reflect.StructField) (string, bool) {
	// at first, find column from tag
	structTag := field.Tag
	tag, _ := structTag.Lookup(g.Tag)

	// if tag is empty, use snake case of Name
	if tag == "" {
		tag = snaker.CamelToSnake(field.Name)
	}

	// check primary flag ( `db:"id,primary"` means primary column )
	ss := strings.Split(tag, ",")
	isPrimary := false
	if len(ss) == 2 {
		if ss[1] == "primary" {
			isPrimary = true
		}
	}

	if ss[0] == "-" {
		// skip tag
		return "", false
	}

	return ss[0], isPrimary
}

func (g Generator) analyzeType(tp reflect.Type, pkg, table string) (map[string]interface{}, error) {
	if tp.Kind() == reflect.Ptr {
		tp = reflect.PtrTo(tp)
	}
	if tp.Kind() != reflect.Struct {
		return nil, fmt.Errorf("unexpected Type: %s", tp.String())
	}
	// note: Now, tp is not pointer type but struct type

	//rep := strings.NewReplacer("a", "", "i", "", "u", "", "e", "", "o", "")
	//shortTable := rep.Replace(table)
	shortTable := table

	primary := []columnInfo{}
	values := []columnInfo{}

	// Field analysis
	for i := 0; i < tp.NumField(); i++ {
		field := tp.Field(i)
		if field.PkgPath != "" {
			// this is unexported field
			continue
		}

		column, isPrimary := g.analyzeColumn(field)
		if column == "" {
			continue
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
	}

	// if there's no primary, firstCol is primary
	if len(primary) == 0 {
		if len(values) != 0 {
			log.Println("There's no primary columns. use first column as primary column:", values[0].Field)
			primary = append(primary, values[0])
			values = values[1:]
		}
	}

	vars := map[string]interface{}{
		"Package":    pkg,
		"Table":      table,
		"Typename":   tp.Name(),
		"TableAlias": shortTable,
		"Primary":    primary,
		"Values":     values,
		"AllColumns": append(primary, values...),
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
	return "{{ .Table }} AS {{ .TableAlias }}"
}

func (p *{{ .Typename }}) Columns() []string {
	return []string{ {{ range $i, $v := .AllColumns }}"{{ $.TableAlias }}.{{ $v.Column }}", {{ end }} }
}

func (p *{{ .Typename }}) PrimaryKeys() []string {
	return []string{ {{ range $i, $v := .Primary }}"{{ $.TableAlias }}.{{ $v.Column }}", {{ end }} }
}

func (p *{{ .Typename }}) PrimaryValues() []interface{} {
	return []interface{}{ {{ range $i, $v := .Primary }}p.{{ $v.Field }}, {{ end }} }
}

func (p *{{ .Typename }}) ValueColumns() []string {
	return []string{ {{ range $i, $v := .Values }}"{{ $.TableAlias }}.{{ $v.Column }}", {{ end }} }
}

func (p *{{ .Typename }}) Values() []interface{} {
	return []interface{}{ {{ range $i, $v := .Values }}p.{{ $v.Field }}, {{ end }} }
}

func (p *{{ .Typename }}) Scan(r seacle.RowScanner) error {
	{{ range $i, $v := .AllColumns }}var arg{{ $i }} {{ $v.Type }}
	{{ end }}
	err := r.Scan({{ range $i, $v := .Values }}&arg{{ $i }}, {{ end }})
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
