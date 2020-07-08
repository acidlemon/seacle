package seacle

import (
	"database/sql"
	"time"
)

type Person struct {
	ID        int64     `db:"id,primary"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
}

func (p *Person) Table() string {
	return "person"
}

func (p *Person) Columns() []string {
	return []string{"id", "name", "created_at"}
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
func (p *Person) Scan(r RowScanner) error {
	var id int64
	var name string
	var createdAt time.Time
	err := r.Scan(&id, &name, &createdAt)
	if err == sql.ErrNoRows {
		return err
	} else if err != nil {
		// something wrong
		return err
	}

	p.ID = id
	p.Name = name
	p.CreatedAt = createdAt

	return nil
}
