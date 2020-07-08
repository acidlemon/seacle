package seacle

import (
	"reflect"
	"testing"
)

func TestGenerator(t *testing.T) {
	gen := Generator{}

	err := gen.Generate(reflect.TypeOf(Person{}), "test", "person", "test/person.gen.go")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

}
