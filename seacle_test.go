package seacle

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

func TestMain(m *testing.M) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	db = setup()

	code := m.Run()

	os.Exit(code)
}

func setup() *sql.DB {
	now := time.Now()
	dbfile := now.Format("/tmp/hoge-20060102150405.db")

	db, err := sql.Open("sqlite3", dbfile)
	if err != nil {
		panic(fmt.Sprintf("failed to open sqlite %s: %s", dbfile, err.Error()))
	}

	// insert initial data
	ctx := context.Background()
	_, err = db.ExecContext(ctx, `CREATE TABLE person (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name VARCHAR(80),
		created_at TIMESTAMP
		)`)
	if err != nil {
		panic(fmt.Sprintf(`failed to crate table: %s`, err))
	}

	_, err = db.ExecContext(ctx, `INSERT INTO person (name, created_at) VALUES 
		("Alberto",    "2018-03-05 12:34:56"),
		("Lamimi",     "2018-04-06 01:23:45"),
		("Naillebert", "2018-05-07 12:34:56"),
		("Blanhaerz",  "2018-06-09 12:34:56"),
		("J'rhoomale", "2018-06-11 12:34:56")`)
	if err != nil {
		panic(fmt.Sprintf(`failed to insert data: %s`, err))
	}

	return db
}

func TestPing(t *testing.T) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		t.Error("failed to ping", err)
	}

	log.Println("Ping OK")
}

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

func TestSelect(t *testing.T) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Errorf("failed to checkout connection: %s", err.Error())
	}

	var people []*Person

	// SELECT 1 row
	people = make([]*Person, 0)
	err = Select(ctx, conn, &people, `WHERE name = ?`, "Lamimi")
	if err != nil {
		t.Errorf("Lamimi is not found: %s", err.Error())
	}
	if len(people) != 1 {
		t.Errorf("len(people) != 1")
	}

	// SELECT 2 rows
	people = make([]*Person, 0)
	err = Select(ctx, conn, &people, `WHERE name LIKE ? ORDER BY id DESC`, "%ber%")
	if err != nil {
		t.Errorf("'%%ber%%' is not found: %s", err.Error())
	}
	if len(people) != 2 {
		t.Errorf("len(people) != 2")
	}
	if people[0].Name != "Naillebert" {
		t.Errorf("order is incorrect: people[0].Name=%s", people[0].Name)
	}
	if people[1].Name != "Alberto" {
		t.Errorf("order is incorrect: people[1].Name=%s", people[1].Name)
	}

	// fail (invalid type)
	people = make([]*Person, 0)
	err = Select(ctx, conn, people, `WHERE name = ?`, "Lamimi")
	if err.Error() != "out is not pointer: []*seacle.Person" {
		t.Errorf("unexpect error: %s", err)
	}

	// success
	pepple := []Person{}
	err = Select(ctx, conn, &pepple, `WHERE name = ?`, "Lamimi")
	if err != nil {
		t.Errorf("Lamimi is not found: %s", err.Error())
	}
	if len(pepple) != 1 {
		t.Errorf("len(pepple) != 1")
	}
	if pepple[0].Name != "Lamimi" {
		t.Errorf("pepple[0].Name is not Lamimi, actual=%s", pepple[0].Name)
	}

}

func TestSelectWhereIn(t *testing.T) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Errorf("failed to checkout connection: %s", err.Error())
	}

	var people []*Person

	// success
	people = make([]*Person, 0)
	err = Select(ctx, conn, &people, `WHERE name IN (?)`, []string{"Alberto", "Blanhaerz"})
	if err != nil {
		t.Errorf("Alberto and Blanhaerz is not found: %s", err.Error())
	}
	if len(people) != 2 {
		t.Errorf("len(people) != 2")
	}

	// fail
	people = make([]*Person, 0)
	err = Select(ctx, conn, &people, `WHERE name IN (?) AND id = ?`, []string{"Alberto", "Blanhaerz"})
	if err == nil {
		t.Errorf("Expect error")
	}
}

func TestSelectRow(t *testing.T) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Errorf("failed to checkout connection: %s", err.Error())
	}

	person := &Person{}
	err = SelectRow(ctx, conn, person, `WHERE name = ?`, "Lamimi")
	if err != nil {
		t.Errorf("Lamimi is not found: %s", err.Error())
	}
	if person.Name != "Lamimi" {
		t.Errorf("Name is not Lamimi, actual=%s", person.Name)
	}
	tm, _ := time.Parse("2006-01-02 15:04:05", "2018-04-06 01:23:45")
	if person.CreatedAt != tm {
		t.Errorf("CreatedAt is not correct, actual=%s", person.CreatedAt)
	}
}

func TestInsertDelete(t *testing.T) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Errorf("failed to checkout connection: %s", err.Error())
	}

	name := "Emet-Selch"
	tm, _ := time.Parse("2006-01-02 15:04:05", "2020-07-07 13:45:00")
	person := &Person{
		Name:      name,
		CreatedAt: tm,
	}
	id, err := Insert(ctx, conn, person)
	if err != nil {
		t.Errorf("failed to insert Emet-Selch: %s", err.Error())
	}
	if id != 6 {
		t.Errorf("unexpected LastInsertID, actual=%d", id)
	}

	newPerson := &Person{}
	err = SelectRow(ctx, conn, newPerson, `WHERE name = ?`, name)
	if err != nil {
		t.Errorf("failed to fetch newPerson: %s", err)
	}
	if newPerson.ID != 6 {
		t.Errorf("unexpected ID, actual=%d", newPerson.ID)
	}

	err = Delete(ctx, conn, newPerson)
	if err != nil {
		t.Errorf("failed to delete newPerson: %s", err)
	}

	err = SelectRow(ctx, conn, newPerson, `WHERE name = ?`, name)
	if err != nil {
		if err != sql.ErrNoRows {
			t.Errorf("unexpected error on SelectRow: %s", err)
		}
	} else {
		t.Errorf("succeeded to fetch for unknown reason, v=%v", newPerson)
	}

}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Errorf("failed to checkout connection: %s", err.Error())
	}

	person := &Person{}
	err = SelectRow(ctx, conn, person, `WHERE name = ?`, "Lamimi")
	if err != nil {
		t.Errorf("failed to fetch Lamimi: %s", err)
	}

	tm, _ := time.Parse("2006-01-02 15:04:05", "2020-07-07 14:25:00")
	person.CreatedAt = tm
	err = Update(ctx, conn, person)
	if err != nil {
		t.Errorf("failed to update Lamimi: %s", err.Error())
	}

	updatedPerson := &Person{}
	err = SelectRow(ctx, conn, updatedPerson, `WHERE name = ?`, "Lamimi")
	if err != nil {
		t.Errorf("failed to fetch Lamimi again: %s", err)
	}
	if updatedPerson.CreatedAt.Unix() != person.CreatedAt.Unix() {
		t.Errorf("unexpected created_at, actual=%v", updatedPerson.CreatedAt)
	}
	if updatedPerson.CreatedAt.Unix() != tm.Unix() {
		t.Errorf("unexpected created_at, actual=%v", updatedPerson.CreatedAt)
	}
}
