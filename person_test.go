package seacle

import (
	"database/sql"
	"time"
)

type Person struct {
	ID        int64     `db:"id,primary,auto_increment"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
}

func (p *Person) Table() string {
	return "person"
}

func (p *Person) Columns() []string {
	return []string{"person.id", "person.name", "person.created_at"}
}

func (p *Person) PrimaryKeys() []string {
	return []string{"id"}
}

func (p *Person) PrimaryValues() []interface{} {
	return []interface{}{p.ID}
}

func (p *Person) ValueColumns() []string {
	return []string{"name", "created_at"}
}

func (p *Person) Values() []interface{} {
	return []interface{}{p.Name, p.CreatedAt}
}

func (p *Person) AutoIncrementColumn() string {
	return "id"
}

func (p *Person) Scan(r RowScanner) error {
	var arg0 int64
	var arg1 string
	var arg2 time.Time

	err := r.Scan(&arg0, &arg1, &arg2)
	if err == sql.ErrNoRows {
		return err
	} else if err != nil {
		// something wrong
		return err
	}

	p.ID = arg0
	p.Name = arg1
	p.CreatedAt = arg2

	return nil
}
