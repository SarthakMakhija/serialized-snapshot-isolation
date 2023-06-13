package errors

import "errors"

var ConflictErr = errors.New("transaction conflicts with other concurrent transaction, retry")
var EmptyTransactionErr = errors.New("transaction is empty, invoke PutOrUpdate in a transaction before committing")
var DuplicateKeyInBatchErr = errors.New("batch already contains the key")
