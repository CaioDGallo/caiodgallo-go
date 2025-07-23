package internal

import (
	"database/sql"
	"log"
	"os"
)

func InitDatabase(instanceID string) (*sql.DB, error) {
	_, err := os.Stat("/data/retry_queue.db")
	if err != nil {
		_, err = os.Create("/data/retry_queue.db")
		if err != nil {
			log.Default().Fatal("failed to create db file", err.Error())
		}
	}

	db, err := sql.Open("sqlite3", "/data/retry_queue.db?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=1000&_busy_timeout=5000&_txlock=immediate")
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(0)

	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS retry_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            retry_count INTEGER DEFAULT 0,
            next_retry DATETIME NOT NULL,
            processing_by TEXT NULL,
            processing_started DATETIME NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_next_retry ON retry_queue(next_retry);
        
				CREATE TABLE IF NOT EXISTS payment_log(
            id INTEGER PRIMARY KEY,
            payment_processor TEXT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_created_at ON payment_log(created_at);
    `)
	if err != nil {
		return nil, err
	}

	db.Exec(`UPDATE retry_queue SET processing_by = NULL WHERE processing_by = ?`, instanceID)

	return db, nil
}
