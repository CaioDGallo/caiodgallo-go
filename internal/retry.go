package internal

import (
	"database/sql"
	"log"
	"time"
)

type RetryHandler struct {
	db              *sql.DB
	retryInsertStmt *sql.Stmt
	InstanceID      string
}

func NewRetryHandler(db *sql.DB) *RetryHandler {
	retryInsertStmt, err := db.Prepare(`
        INSERT INTO retry_queue (payload, next_retry) VALUES (?, datetime('now', '+30 seconds'))
    `)
	if err != nil {
		log.Fatal("unable to prepare retry statement", err.Error())
	}

	return &RetryHandler{
		db:              db,
		retryInsertStmt: retryInsertStmt,
	}
}

func (rh *RetryHandler) EnqueueRetry(payload []byte) error {
	_, err := rh.retryInsertStmt.Exec(string(payload))
	if err != nil {
		log.Printf("Failed to enqueue retry: %v", err)
		return err
	}

	return nil
}

func (rh *RetryHandler) ProcessRetryQueue() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	stmt, _ := rh.db.Prepare(`
        UPDATE retry_queue 
        SET processing_by = ?, processing_started = datetime('now')
        WHERE id IN (
            SELECT id FROM retry_queue 
            WHERE next_retry <= datetime('now') 
            AND processing_by IS NULL 
            LIMIT 10
        )
    `)
	defer stmt.Close()

	for range ticker.C {
		// Claim items
		result, err := stmt.Exec(rh.InstanceID)
		if err != nil {
			continue
		}

		affected, _ := result.RowsAffected()
		if affected == 0 {
			continue
		}

		// Process claimed items
		rows, err := rh.db.Query(`
            SELECT id, payload FROM retry_queue 
            WHERE processing_by = ? AND processing_started >= datetime('now', '-1 minute')`,
			rh.InstanceID)
		if err != nil {
			continue
		}

		for rows.Next() {
			var id int64
			var payload string
			if err := rows.Scan(&id, &payload); err != nil {
				continue
			}

			success := rh.processRetryItem(payload)

			if success {
				rh.db.Exec("DELETE FROM retry_queue WHERE id = ?", id)
			} else {
				rh.db.Exec(`
                    UPDATE retry_queue 
                    SET retry_count = retry_count + 1,
                        next_retry = datetime('now', '+' || (1 << (retry_count + 1)) || ' minutes'),
                        processing_by = NULL
                    WHERE id = ? AND retry_count < 5`, id)
			}
		}
		rows.Close()
	}
}

func (rh *RetryHandler) processRetryItem(payload string) bool {
	// Implement your retry logic here
	// Return true if successful, false to retry
	return true
}

func (rh *RetryHandler) Cleanup() {
	rh.retryInsertStmt.Close()
}
