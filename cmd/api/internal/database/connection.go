package database

import (
	"database/sql"
	"time"

	_ "github.com/lib/pq"
)

type PreparedStatements struct {
	Insert    *sql.Stmt
	Update    *sql.Stmt
	StatsAll  *sql.Stmt
	StatsFrom *sql.Stmt
	StatsTo   *sql.Stmt
	StatsBoth *sql.Stmt
}

func SetupDatabase(connStr string) (*sql.DB, *PreparedStatements, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, nil, err
	}

	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(3 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS payment_log(
			id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			amount DECIMAL NOT NULL,
			fee DECIMAL NOT NULL,
			payment_processor TEXT,
			requested_at TIMESTAMP,
			processing_started_at TIMESTAMP,
			status TEXT DEFAULT 'success',
			idempotency_key TEXT UNIQUE
		);
		CREATE INDEX IF NOT EXISTS idx_requested_at ON payment_log(requested_at);
		CREATE INDEX IF NOT EXISTS idx_status ON payment_log(status);
		CREATE INDEX IF NOT EXISTS idx_status_requested_at ON payment_log(status, requested_at);
		CREATE INDEX IF NOT EXISTS idx_pending_failed_processing ON payment_log(status, processing_started_at) WHERE status IN ('pending', 'failed');
    `)
	if err != nil {
		return nil, nil, err
	}

	insertStmt, err := db.Prepare(InsertPayment)
	if err != nil {
		return nil, nil, err
	}

	updateStmt, err := db.Prepare(UpdatePayment)
	if err != nil {
		insertStmt.Close()
		return nil, nil, err
	}

	statsAllStmt, err := db.Prepare(StatsAll)
	if err != nil {
		insertStmt.Close()
		updateStmt.Close()
		return nil, nil, err
	}

	statsFromStmt, err := db.Prepare(StatsFrom)
	if err != nil {
		insertStmt.Close()
		updateStmt.Close()
		statsAllStmt.Close()
		return nil, nil, err
	}

	statsToStmt, err := db.Prepare(StatsTo)
	if err != nil {
		insertStmt.Close()
		updateStmt.Close()
		statsAllStmt.Close()
		statsFromStmt.Close()
		return nil, nil, err
	}

	statsBothStmt, err := db.Prepare(StatsBoth)
	if err != nil {
		insertStmt.Close()
		updateStmt.Close()
		statsAllStmt.Close()
		statsFromStmt.Close()
		statsToStmt.Close()
		return nil, nil, err
	}

	stmts := &PreparedStatements{
		Insert:    insertStmt,
		Update:    updateStmt,
		StatsAll:  statsAllStmt,
		StatsFrom: statsFromStmt,
		StatsTo:   statsToStmt,
		StatsBoth: statsBothStmt,
	}

	return db, stmts, nil
}

func (ps *PreparedStatements) Close() {
	if ps.Insert != nil {
		ps.Insert.Close()
	}
	if ps.Update != nil {
		ps.Update.Close()
	}
	if ps.StatsAll != nil {
		ps.StatsAll.Close()
	}
	if ps.StatsFrom != nil {
		ps.StatsFrom.Close()
	}
	if ps.StatsTo != nil {
		ps.StatsTo.Close()
	}
	if ps.StatsBoth != nil {
		ps.StatsBoth.Close()
	}
}