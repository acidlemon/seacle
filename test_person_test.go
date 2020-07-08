package seacle

import (
	"time"

	"github.com/google/uuid"
)

type TestPerson struct {
	ID        int64     `db:"id,primary"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
	SerialID  uuid.UUID `db:"uuid"`
}
