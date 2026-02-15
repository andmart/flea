# FleaStore

FleaStore is a small, embeddable storage engine written in Go.

It is designed to be easy to understand and easy to use.  
Rather than trying to be a full database, FleaStore focuses on a minimal and explicit feature set that works well for many in-process use cases.

Typical use cases include:
- Embedded storage inside applications
- Small datasets that must survive restarts
- Testing and prototyping

FleaStore is a library, not a standalone server.

------------------------------------------------------------------------

## About the examples

Throughout this document, examples use a simple `User` type to illustrate how the API is used:

``` go
type User struct {
    Id   uint64
    Name string
    Age  int
}
```

`User` is only an example.\
FleaStore works with **any** type.

------------------------------------------------------------------------

## Overview

At its core, FleaStore stores values of any type and identifies them
using a function provided by the user.

``` go
Store[ID, T]
```

-   `ID` is the identifier type
-   `T` is the value being stored

The store:

-   Preserves insertion order
-   Uses append-only persistence
-   Supports logical deletion
-   Supports physical offloading of data to disk (offline state)

------------------------------------------------------------------------

## Identity and IDFunc

Every value stored in FleaStore has an ID.

The ID is computed using a user-provided function:

``` go
type IDFunc[ID, T any] func(T) (ID, error)
```

This function:
- Is required
- Must always return the same ID for the same value
- Fully defines how records are identified

FleaStore does not generate hidden IDs or keys.  
If two values produce the same ID, they refer to the same record.

------------------------------------------------------------------------

## Opening a Store

A store is created using `Open`:

``` go
store, err := Open[uint64, User](Options[uint64, User]{
    Dir: "...",
    IDFunc: func(u User) (uint64, error) {
        return u.Id, nil
    },
})
```

When a store is opened:
- Existing data is recovered from disk (if present)
- The in-memory state is rebuilt
- The store becomes ready for use

If `Open` returns an error, the store was not created.

------------------------------------------------------------------------

## Options

`Options` controls how a Store is configured when it is opened.

``` go
type Options[ID comparable, T any] struct {
    Dir              string
    SnapshotInterval time.Duration
    IDFunc           IDFunc[ID, T]
    Checkers         []Checker[T]

    ResidencyFunc    ResidencyFunc[T]
    MaxOnline        *int
}
```

------------------------------------------------------------------------

### Dir

Directory used for persistence. If not provided, current dir will be used.

Layout:

    /data/<model>/
      snapshot.ndjson
      wal.log
      data.ndjson

------------------------------------------------------------------------

### SnapshotInterval (optional)

``` go 
SnapshotInterval time.Duration
```

Defines how often a snapshot is created. If not provided, it defaults to 30s.


------------------------------------------------------------------------

### IDFunc (required)

```go
IDFunc IDFunc[ID, T]
```

`IDFunc` defines how values are identified in the store.

Rules:
- This field is mandatory
- The function must be deterministic
- Identity depends exclusively on this function

------------------------------------------------------------------------

### Checkers (optional)

```go
Checkers []Checker[T]
```

Checkers allow validation or transformation of values before they are written.

They can be used to:
- Reject invalid data
- Normalize values
- Enforce simple rules

If no checkers are provided, values are written as-is.

Checkers are applied only to new write operations and are not executed during recovery.


## Residency


Residency controls which records remain in memory.

### ResidencyFunc (optional)

Defines whether a record should remain in memory or be moved to the disk.

If it returns `false`, the record may be moved to the disk.

### MaxInMemoryRecords (optional)

Limits how many records remain in memory.

-   `nil` → unlimited
-   `-1` → residency always allowed to run
-   `>0` → caps the number of in-memory records. ResidencyFunc will not run if the limit is not exceeded.

------------------------------------------------------------------------

## Writing Data

### Put

``` go
id, err := store.Put(value)
```

`Put` inserts or updates a value:

- The ID is computed using `IDFunc`
- If the ID does not exist, the value is inserted
- If the ID already exists, the value is updated
- The order of insertion is preserved

Errors may be returned if:
- The ID function fails
- A checker rejects the value

------------------------------------------------------------------------

### PutAll

``` go
ids, err := store.PutAll(values)
```

`PutAll` writes multiple values at once.

It behaves like calling `Put` for each value, but is more efficient when handling many items.

All values are processed in order.  
If an error occurs, no changes are applied.

This method is useful for:
- Bulk inserts
- Initial data loading
- Tests

Offline data is not stored in WAL.

------------------------------------------------------------------------

## Reading Data

### Get

``` go
results, err := store.Get(predicate)
```

`Get`:

-   Scans in insertion order
-   Ignores logically deleted records
-   Evaluates both:
    -   Online records (in memory)
    -   Offline records (loaded in chunks from disk)

Example:

```go
adults := store.Get(func(u User) bool {
    return u.Age >= 18
})
```

`Get` may perform disk I/O if offline data exists.

------------------------------------------------------------------------

## Delete

``` go
deleted, err := store.Delete(predicate)
```

`Delete` removes values that match a predicate.

- All matching values are logically deleted
- Deleted values are returned to the caller
- The operation is persisted and survives restarts

If no values match the predicate, the operation succeeds and returns an empty slice.

------------------------------------------------------------------------


## Predicates

Predicates are simple filter functions:

```go
type Predicate[T any] func(T) bool
```

They should:
- Be fast
- Avoid side effects
- Not modify the value

------------------------------------------------------------------------

## Checkers

Checkers allow you to validate or adjust values before they are written.

``` go
type Checker[T any] func(old *T, new T) (*T, error)
```

-   Can reject writes
-   Can transform values
-   Only applied during new writes
-   Not executed during recovery


Checkers are only applied to new writes and updates.  
Recovered data is restored exactly as it was written.

------------------------------------------------------------------------

## Persistence Model

FleaStore uses:

### WAL

-   Append-only
-   Contains only Put and Delete operations
-   Used only for crash recovery
-   Truncated after successful replay
-   Does not contain offline data

### Snapshot

-   Speeds up startup
-   Compatible with WAL
-   Respects residency limits

### Offline Data

-   Stored in `data.ndjson`
-   Append-only
-   Loaded on demand during `Get`

------------------------------------------------------------------------

## Concurrency

FleaStore is safe for concurrent use.

Internal operations are protected by mutex.

To maintain predictability:

-   Avoid long-running predicates
-   Avoid blocking work inside checkers

------------------------------------------------------------------------

## Design Principles

FleaStore intentionally avoids:

-   Relational features
-   Secondary indexes
-   LSM trees
-   Query languages
-   Hidden background compaction
-   Automatic magic

It favors:

-   Explicit control
-   Predictable behavior
-   Append-only persistence
-   Simple data structures

------------------------------------------------------------------------

## Status

FleaStore is experimental and evolving.

The API may change, but changes are made carefully and with a focus on clarity.

------------------------------------------------------------------------

## License

MIT
