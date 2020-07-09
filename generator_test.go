package seacle

import (
	"reflect"
	"testing"
)

func TestGenerator(t *testing.T) {
	gen := Generator{
		Tag: "db",
	}

	err := gen.Generate(reflect.TypeOf(TestPerson{}), "seacle", "person", "test/test_person.gen.go")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	err = gen.Generate(reflect.TypeOf(TestPerson2{}), "seacle", "person", "test/test_person2.gen.go")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	err = gen.Generate(reflect.TypeOf(TestPerson3{}), "seacle", "person", "test/test_person3.gen.go")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}
