// Here be option functions for constructing a new Worker.

package worker

import (
	"errors"
	"time"

	"github.com/rounds/go-bqstreamer/lib"
)

const (
	DefaultErrorBufferSize  = 20
	DefaultMaxRetryInsert   = 3
	DefaultMaxRows          = 500
	DefaultMaxDelay         = 5 * time.Second
	DefaultSleepBeforeRetry = 5 * time.Second
)

type OptionFunc func(*Worker) error

// SetErrorBufferSize sets the error channel size.
//
// NOTE value must be a non-negative int.
func SetErrorBufferSize(size int) OptionFunc {
	return func(w *Worker) error {
		if size < 0 {
			return errors.New("error channel size must be a non-negative int")
		}
		w.ErrorChan = make(chan error, size)
		return nil
	}
}

// SetErrorChannel sets the Worker's error channel.
//
// This function is used when we need multiple workers to report errors to the
// same channel in a "fan-in" model, to be processed together.
func SetErrorChannel(errChan chan error) OptionFunc {
	return func(w *Worker) error {
		if errChan == nil {
			return errors.New("error channel is nil")
		}
		w.ErrorChan = errChan
		return nil
	}
}

// SetMaxRetryInsert sets the maximum amount of retries a failed insert is
// retried before dropping the rows and moving on (giving up on the insert).
//
// NOTE value must be a non-negative int.
func SetMaxRetryInsert(retries int) OptionFunc {
	return func(w *Worker) error {
		if retries < 0 {
			return errors.New("max retry insert must be a non-negative int")
		}
		w.MaxRetryInsert = retries
		return nil
	}
}

// SetMaxRows sets the maximum amount of rows to be queued in a table before an
// insert takes place.
//
// NOTE value must be a positive int.
func SetMaxRows(rowLen int) OptionFunc {
	return func(w *Worker) error {
		if rowLen <= 0 {
			return errors.New("max rows must be a positive int")
		}
		w.RowChan = make(chan *lib.Row, rowLen)
		w.Rows = make([]*lib.Row, 0, rowLen)
		return nil
	}
}

// SetRowChannel sets the Worker's row channel.
//
// This function is used when we need multiple workers to fetch rows from the
// same channel in a "fan-out" way.
func SetRowChannel(rowChan chan *lib.Row) OptionFunc {
	return func(w *Worker) error {
		if rowChan == nil {
			return errors.New("row channel is nil")
		}
		w.RowChan = rowChan
		return nil
	}
}

// SetMaxDelay sets the maximum time delay before an insert takes place for all
// tables.
//
// NOTE value must be a positive time.Duration.
func SetMaxDelay(delay time.Duration) OptionFunc {
	return func(w *Worker) error {
		if delay <= 0 {
			return errors.New("max delay must be a positive time.Duration")
		}
		w.MaxDelay = delay
		return nil
	}
}

// SetSleepBeforeRetry sets the time delay before retrying a rejected or
// failed insert (if required).
//
// NOTE value must be a positive time.Duration.
func SetSleepBeforeRetry(sleep time.Duration) OptionFunc {
	return func(w *Worker) error {
		if sleep <= 0 {
			return errors.New("sleep before retry must be a positive time.Duration")
		}
		w.SleepBeforeRetry = sleep
		return nil
	}
}
