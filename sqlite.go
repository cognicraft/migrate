package migrate

import (
	"database/sql"
	"time"
)

var (
	_ Support = SQLiteSupport{}
)

type SQLiteSupport struct{}

func (SQLiteSupport) ExistsMigrationsTable(db *sql.DB) (bool, error) {
	var exists bool
	row := db.QueryRow(`SELECT count(tbl_name) FROM sqlite_master WHERE type='table' AND tbl_name='migrations';`)
	err := row.Scan(&exists)
	return exists, err
}

func (SQLiteSupport) CreateMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(sqliteMigrations)
	return err
}

func (SQLiteSupport) Clean(db *sql.DB) error {
	var err error
	_, err = db.Exec(`PRAGMA writable_schema = 1;`)
	_, err = db.Exec(`DELETE FROM sqlite_master WHERE type in ('table', 'index', 'trigger');`)
	_, err = db.Exec(`PRAGMA writable_schema = 0;`)
	_, err = db.Exec(`VACUUM;`)
	return err
}

func (SQLiteSupport) RecordMigration(db *sql.DB, m Migration) error {
	_, err := db.Exec(`INSERT INTO migrations (rank, version, description, type, checksum, date, execution_time, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		m.Rank,
		string(m.Version),
		m.Description,
		string(m.Type),
		m.Checksum,
		m.Date.Format(time.RFC3339),
		int64(m.ExecutionTime),
		string(m.Status),
	)
	return err
}

func (SQLiteSupport) ListMigrations(con *sql.DB) (Migrations, error) {
	rows, err := con.Query(`SELECT rank, version, description, type, checksum, date, execution_time, status FROM migrations;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ms := []Migration{}
	for rows.Next() {
		var rank int
		var version string
		var description string
		var typ string
		var checksum string
		var date string
		var execution_time int
		var status string
		err := rows.Scan(&rank, &version, &description, &typ, &checksum, &date, &execution_time, &status)
		if err != nil {
			return nil, err
		}
		d, _ := time.Parse(time.RFC3339, date)
		m := Migration{
			Rank:          rank,
			Version:       Version(version),
			Description:   description,
			Type:          Type(typ),
			Checksum:      checksum,
			Date:          d,
			ExecutionTime: execution_time,
			Status:        Status(status),
		}
		ms = append(ms, m)
	}
	return ms, nil
}

const sqliteMigrations = `
CREATE TABLE migrations (
  rank INTEGER NOT NULL,
  version TEXT NOT NULL,
  description TEXT NOT NULL,
  type TEXT NOT NULL,
  checksum TEXT,
  date TEXT NOT NULL,
  execution_time INTEGER NOT NULL,
  status TEXT NOT NULL,
  PRIMARY KEY (rank)
);`
