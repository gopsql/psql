package psql

import (
	"context"
	"errors"
	"fmt"

	"github.com/gopsql/db"
)

type (
	TransactionBlock func(context.Context, db.Tx) error
)

// MustTransaction starts a transaction, uses context.Background() internally
// and panics if transaction fails.
func (m Model) MustTransaction(block TransactionBlock) {
	if err := m.Transaction(block); err != nil {
		panic(err)
	}
}

// Transaction starts a transaction, uses context.Background() internally.
func (m Model) Transaction(block TransactionBlock) error {
	return m.TransactionCtx(context.Background(), block)
}

// MustTransactionCtx starts a transaction and panics if transaction fails.
func (m Model) MustTransactionCtx(ctx context.Context, block TransactionBlock) {
	if err := m.TransactionCtx(ctx, block); err != nil {
		panic(err)
	}
}

// TransactionCtx starts a transaction.
func (m Model) TransactionCtx(ctx context.Context, block TransactionBlock) (err error) {
	m.log("BEGIN", nil, 0)
	var tx db.Tx
	tx, err = m.connection.BeginTx(ctx, "", false)
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			m.log("ROLLBACK", nil, 0)
			tx.Rollback(ctx)
			if rerr, ok := r.(error); ok {
				err = rerr
			} else {
				err = errors.New(fmt.Sprint(r))
			}
		} else if err != nil {
			m.log("ROLLBACK", nil, 0)
			tx.Rollback(ctx)
		} else {
			m.log("COMMIT", nil, 0)
			err = tx.Commit(ctx)
		}
	}()
	err = block(ctx, tx)
	return
}
