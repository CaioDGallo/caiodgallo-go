package internal

import (
	"database/sql"
	"log"
	"time"
)

const (
	countBoth = "SELECT COUNT(*) FROM payment_log WHERE created_at >= ? AND created_at <= ?"
	countFrom = "SELECT COUNT(*) FROM payment_log WHERE created_at >= ?"
	countTo   = "SELECT COUNT(*) FROM payment_log WHERE created_at <= ?"
	countAll  = "SELECT COUNT(*) FROM payment_log"
)

type PreparedGetPaymentQueries struct {
	countBoth *sql.Stmt
	countFrom *sql.Stmt
	countTo   *sql.Stmt
	countAll  *sql.Stmt
}

type PaymentLogHandler struct {
	db                        *sql.DB
	paymentLogInsertStmt      *sql.Stmt
	preparedGetPaymentQueries *PreparedGetPaymentQueries
	InstanceID                string
}

func NewPaymentLogHandler(db *sql.DB) *PaymentLogHandler {
	var err error
	paymentLogInsertStmt, err := db.Prepare(`
        INSERT INTO payment_log (payment_processor) VALUES (?)
    `)
	if err != nil {
		log.Fatal("unable to prepare payment_log insert statement", err.Error())
	}

	pq := &PreparedGetPaymentQueries{}

	pq.countBoth, err = db.Prepare(countBoth)
	if err != nil {
		log.Fatal("unable to prepare payment_log get_payment from and to statement", err.Error())
	}

	pq.countFrom, err = db.Prepare(countFrom)
	if err != nil {
		log.Fatal("unable to prepare payment_log get_payment from statement", err.Error())
	}

	pq.countTo, err = db.Prepare(countTo)
	if err != nil {
		log.Fatal("unable to prepare payment_log get_payment to statement", err.Error())
	}

	pq.countAll, err = db.Prepare(countAll)
	if err != nil {
		log.Fatal("unable to prepare payment_log get_payment all statement", err.Error())
	}

	return &PaymentLogHandler{
		db:                        db,
		paymentLogInsertStmt:      paymentLogInsertStmt,
		preparedGetPaymentQueries: pq,
	}
}

func (plh *PaymentLogHandler) RegisterPayment(pp string) error {
	_, err := plh.paymentLogInsertStmt.Exec(pp)
	if err != nil {
		log.Printf("Failed to register payment: %v", err)
		return err
	}

	return nil
}

func (plh *PaymentLogHandler) GetPaymentCount(from, to time.Time) (int, error) {
	hasFrom := !from.IsZero()
	hasTo := !to.IsZero()

	var row *sql.Row

	switch {
	case hasFrom && hasTo:
		row = plh.preparedGetPaymentQueries.countBoth.QueryRow(from, to)
	case hasFrom:
		row = plh.preparedGetPaymentQueries.countFrom.QueryRow(from)
	case hasTo:
		row = plh.preparedGetPaymentQueries.countTo.QueryRow(to)
	default:
		row = plh.preparedGetPaymentQueries.countAll.QueryRow()
	}

	var count int
	err := row.Scan(&count)
	if err != nil {
		log.Printf("Failed to register payment: %v", err)
		return 0, err
	}
	return count, nil
}

func (plh *PaymentLogHandler) Cleanup() {
	plh.paymentLogInsertStmt.Close()
}
