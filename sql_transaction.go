package psql

import (
	"context"
	"errors"
	"fmt"
)

type (
	// TransactionBlock is a function executed within a database transaction.
	// Return nil to commit; return an error or panic to rollback.
	TransactionBlock func(context.Context, Tx) error
)

// MustTransaction is like Transaction but panics if the transaction fails.
func (m Model) MustTransaction(block TransactionBlock) {
	if err := m.Transaction(block); err != nil {
		panic(err)
	}
}

// Transaction executes the block within a database transaction. The transaction
// is committed if block returns nil; it is rolled back if block returns an error
// or panics. Uses context.Background internally; use TransactionCtx for custom
// contexts.
func (m Model) Transaction(block TransactionBlock) error {
	return m.TransactionCtx(context.Background(), block)
}

// MustTransactionCtx is like TransactionCtx but panics if the transaction fails.
func (m Model) MustTransactionCtx(ctx context.Context, block TransactionBlock) {
	if err := m.TransactionCtx(ctx, block); err != nil {
		panic(err)
	}
}

// TransactionCtx executes the block within a database transaction with the
// given context. The transaction is committed if block returns nil; it is
// rolled back if block returns an error or panics.
//
//	users.TransactionCtx(ctx, func(ctx context.Context, tx db.Tx) error {
//		users.Insert("Name", "Alice").MustExecuteCtxTx(ctx, tx)
//		users.Insert("Name", "Bob").MustExecuteCtxTx(ctx, tx)
//		return nil // commit
//	})
func (m Model) TransactionCtx(ctx context.Context, block TransactionBlock) (err error) {
	m.log("BEGIN", nil, 0)
	var tx Tx
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
