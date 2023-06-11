package errors

import "errors"

var ConflictErr = errors.New("transaction conflicts with other")
var EmptyTransactionErr = errors.New("transaction is empty, invoke PutOrUpdate in a transaction before committing")
