package sqltor

import (
	"errors"
	"fmt"
)

var ErrTooManyArgs = errors.New("too many args")

func TooManyArgs(provided, needed int) error {
	return fmt.Errorf("%w: provided %v, needed %v", ErrTooManyArgs, provided, needed)
}

var ErrNotEnoughArgs = errors.New("not enough arguments")

func NotEnoughArgs(provided, needed int) error {
	return fmt.Errorf("%w: provided %v, needed at least %v", ErrNotEnoughArgs, provided, needed)
}

var ErrNoFilters = errors.New("no filters passed to function")

func NoFiltersErr() error {
	return ErrNoFilters
}

var ErrFilterDoesNotExist = errors.New("filter does not exist")

func FilterDoesNotExistErr(filter string) error {
	return fmt.Errorf("%w: %s", ErrFilterDoesNotExist, filter)
}

var ErrZeroTables = errors.New("zero tables")

func ZeroTablesErr(filter string) error {
	return fmt.Errorf("%w: filter %s contains 0 tables in it", ErrZeroTables, filter)
}

var ErrCannotJoin = errors.New("cannot join tables")

func CannotJoinTablesErr(table, availableTables string) error {
	return fmt.Errorf("%w: cannot join table %v on unknown conditions, have tables %v", ErrCannotJoin, table, availableTables)
}

var ErrCannotConvertToString = errors.New("cannot convert interface to string")

func CannotConvertErr(value interface{}) error {
	return fmt.Errorf("%w: %v", ErrCannotConvertToString, value)
}
