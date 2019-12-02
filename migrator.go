package migrate

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"time"
)

const (
	placeholderPrefix = "{"
	placeholderSuffix = "}"
)

type LogFunc func(format string, args ...interface{})

func NewMigrator(log LogFunc, db *sql.DB, support Support) *Migrator {
	return &Migrator{
		log:     log,
		db:      db,
		support: support,
	}
}

type Migrator struct {
	log        LogFunc
	db         *sql.DB
	support    Support
	migrations Migrations
	repeatable Migrations
}

func (m *Migrator) Add(mig Migration) {
	if mig.IsRepeatable() {
		m.repeatable = append(m.repeatable, mig)
	} else {
		m.migrations = append(m.migrations, mig)
	}
}

func (m *Migrator) AddSQLMigration(version Version, description string, script string) {
	m.Add(Migration{
		Version:     version,
		Description: description,
		Type:        TypeSQL,
		Checksum:    SQLChecksum(script),
		Execute: func(db *sql.DB) error {
			for _, stmt := range Statements(script) {
				if _, err := db.Exec(stmt); err != nil {
					return err
				}
			}
			return nil
		},
	})
}

func (m *Migrator) AddRepeatableSQLMigration(description string, script string) {
	m.AddSQLMigration(VersionRepeatable, description, script)
}

func (m *Migrator) AddGoMigration(version Version, description string, execute CommandFunc) {
	m.Add(Migration{
		Version:     version,
		Description: description,
		Type:        TypeGo,
		Execute:     execute,
	})
}

func (m *Migrator) AddRepeatableGoMigration(description string, execute CommandFunc) {
	m.AddGoMigration(VersionRepeatable, description, execute)
}

// create metadata table if not exists
// apply missing migrations
func (m *Migrator) Migrate() error {
	exists, err := m.support.ExistsMigrationsTable(m.db)
	if err != nil {
		return err
	}
	if !exists {
		if err := m.support.CreateMigrationsTable(m.db); err != nil {
			return err
		}
	}
	installed, err := m.support.ListMigrations(m.db)
	if err != nil {
		return err
	}
	rank := 0
	lastInstalled := VersionNone
	checksumsRepeatable := map[string]string{}
	for _, mig := range installed {
		if mig.IsRepeatable() {
			checksumsRepeatable[mig.Description] = mig.Checksum
		} else {
			switch mig.Status {
			case StatusFailed:
				return fmt.Errorf("detected a failed migration: %s", mig)
			case StatusSuccess:
				lastInstalled = mig.Version
			default:
				return fmt.Errorf("unknown status in migration: %s", mig)
			}
		}
		rank = mig.Rank
	}
	// install pending
	for _, mig := range m.migrations {
		if LEQ(mig.Version, lastInstalled) {
			m.log("skipping installed migration: %s - %s", mig.Version, mig.Description)
			continue
		}
		rank++
		mig.Rank = rank
		if err := m.install(mig); err != nil {
			return err
		}
	}
	// install repeatable
	for _, mig := range m.repeatable {
		if cs, exists := checksumsRepeatable[mig.Description]; exists && cs == mig.Checksum {
			m.log("skipping repeatable migration: %s", mig.Description)
			continue
		}
		rank++
		mig.Rank = rank
		if err := m.install(mig); err != nil {
			return err
		}
	}
	return nil
}

// Drops all objects in configured schemas
// Clean is a great help in development and test. It will effectively give you a fresh start, by wiping your configured schemas completely clean. All objects (tables, views, procedures, ...) will be dropped.
// Needless to say: do not use against your production DB!
func (m *Migrator) Clean() error {
	return m.support.Clean(m.db)
}

// The details and status information about all the migrations.
// List lets you know where you stand. At a glance you will see which migrations have already been applied, which other ones are still pending, when they were executed and whether they were successful or not.
func (m *Migrator) Info() Info {
	ms, err := m.support.ListMigrations(m.db)
	if err != nil {
		m.log("error: %v", err)
	}
	return Info{
		Migrations: ms,
	}
}

// Baselines an existing database, excluding all migrations upto and including baselineVersion.
// Baseline is for introducing Migrator to existing databases by baselining them at a specific version. The will cause Migrate to ignore all migrations upto and including the baseline version. Newer migrations will then be applied as usual.
func (m *Migrator) Baseline(version Version, description string) error {
	exists, err := m.support.ExistsMigrationsTable(m.db)
	if err != nil {
		return err
	}
	if !exists {
		if err := m.support.CreateMigrationsTable(m.db); err != nil {
			return err
		}
	}
	installed, err := m.support.ListMigrations(m.db)
	if err != nil {
		return err
	}
	if len(installed) > 0 {
		return fmt.Errorf("unable to baseline: found existing migrations")
	}
	mig := Migration{
		Rank:        1,
		Version:     version,
		Description: description,
		Type:        TypeBaseline,
		Date:        time.Now().UTC(),
		Status:      StatusSuccess,
	}
	m.support.RecordMigration(m.db, mig)
	return m.Migrate()
}

// Validates the applied migrations against the available ones.
// Validate helps you verify that the migrations applied to the database match the ones available locally.
// This is very useful to detect accidental changes that may prevent you from reliably recreating the schema.
func (m *Migrator) Validate() {

}

// Repairs the metadata table
// Repair is your tool to fix issues with the metadata table. It has two main uses:
// - Remove failed migration entries (only for databases that do NOT support DDL transactions)
func (m *Migrator) Repair() {

}

func (m *Migrator) install(mig Migration) error {
	if mig.Execute == nil {
		return fmt.Errorf("cannot execute migration: %s", mig)
	}
	m.log("installing: %s", mig)
	mig.Date = time.Now().UTC()
	err := mig.Execute(m.db)
	mig.ExecutionTime = int(time.Since(mig.Date) / time.Millisecond)
	if err == nil {
		mig.Status = StatusSuccess
	} else {
		mig.Status = StatusFailed
	}
	if rErr := m.support.RecordMigration(m.db, mig); rErr != nil {
		return fmt.Errorf("record migration: %s: %+v", mig, rErr)
	}
	return err
}

type Migration struct {
	Rank          int
	Version       Version
	Description   string
	Type          Type
	Checksum      string
	Date          time.Time
	ExecutionTime int
	Status        Status
	Execute       CommandFunc `json:"-"`
}

func (m Migration) IsRepeatable() bool {
	return m.Version == VersionRepeatable
}

func (m Migration) String() string {
	return fmt.Sprintf("@Migration|version=%s|description=%s|type=%s",
		m.Version,
		m.Description,
		m.Type,
	)
}

type Migrations []Migration

func (ms Migrations) String() string {
	buf := &bytes.Buffer{}
	for _, m := range ms {
		buf.WriteString(m.String())
		buf.WriteString("\n")
	}
	return buf.String()
}

type Support interface {
	ExistsMigrationsTable(con *sql.DB) (bool, error)
	CreateMigrationsTable(con *sql.DB) error
	RecordMigration(con *sql.DB, m Migration) error
	ListMigrations(con *sql.DB) (Migrations, error)
	Clean(con *sql.DB) error
}

type Version string

func LEQ(a Version, b Version) bool {
	ai, _ := strconv.ParseInt(string(a), 10, 64)
	bi, _ := strconv.ParseInt(string(b), 10, 64)
	return ai <= bi
}

const (
	VersionNone       Version = ""
	VersionRepeatable Version = "R"
)

type Status string

const (
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
)

type Type string

const (
	TypeGo       Type = "Go"
	TypeSQL      Type = "SQL"
	TypeBaseline Type = "Baseline"
)

type Info struct {
	Migrations Migrations
}

type CommandFunc func(con *sql.DB) error

func SQLChecksum(script string) string {
	h := md5.New()
	io.WriteString(h, script)
	return fmt.Sprintf("%x", h.Sum(nil))
}
