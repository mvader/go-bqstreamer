// Here be option functions for constructing a new Streamer.

package async

import (
	"errors"
	"time"

	"github.com/rounds/go-bqstreamer/async/worker"
)

const (
	DefaultNumWorkers       = 10
	DefaultErrorBufferSize  = worker.DefaultErrorBufferSize
	DefaultMaxRetryInsert   = worker.DefaultMaxRetryInsert
	DefaultMaxRows          = worker.DefaultMaxRows
	DefaultMaxDelay         = worker.DefaultMaxDelay
	DefaultSleepBeforeRetry = worker.DefaultSleepBeforeRetry
)

type OptionFunc func(*Streamer) error

// SetNumWorkers sets the amount of background workers.
//
// NOTE value must be a positive int.
func SetNumWorkers(workers int) OptionFunc {
	return func(s *Streamer) error {
		if workers <= 0 {
			return errors.New("number of workers must be a positive int")
		}
		s.numWorkers = workers
		return nil
	}
}

// SetErrorBufferSize sets the error channel size.
//
// NOTE value must be a non-negative int.
func SetErrorBufferSize(size int) OptionFunc {
	return func(s *Streamer) error {
		if size < 0 {
			return errors.New("error channel size must be a non-negative int")
		}
		s.errorBufferSize = size
		return nil
	}
}

// SetMaxRetryInsert sets the maximum amount of retries a failed insert is
// retried before dropping the rows and moving on (giving up on the insert).
//
// NOTE value must be a non-negative int.
func SetMaxRetryInsert(retries int) OptionFunc {
	return func(s *Streamer) error {
		if retries < 0 {
			return errors.New("max retry insert must be a non-negative int")
		}
		s.maxRetryInsert = retries
		return nil
	}
}

// SetMaxRows sets the maximum amount of rows to be queued in a table before an
// insert takes place.
//
// NOTE value must be a non-negative int.
func SetMaxRows(rowLen int) OptionFunc {
	return func(s *Streamer) error {
		if rowLen <= 0 {
			return errors.New("max rows must be non-negative int")
		}
		s.maxRows = rowLen
		return nil
	}
}

// SetMaxDelay sets the maximum time delay before an insert takes place for all
// tables.
//
// NOTE value must be a positive time.Duration.
func SetMaxDelay(delay time.Duration) OptionFunc {
	return func(s *Streamer) error {
		if delay <= 0 {
			return errors.New("max delay must be a positive time.Duration")
		}
		s.maxDelay = delay
		return nil
	}
}

// SetSleepBeforeRetry sets the time delay before retrying a rejected or
// failed insert (if required).
//
// NOTE value must be a positive time.Duration.
func SetSleepBeforeRetry(sleep time.Duration) OptionFunc {
	return func(s *Streamer) error {
		if sleep <= 0 {
			return errors.New("sleep before retry must be a positive time.Duration")
		}
		s.sleepBeforeRetry = sleep
		return nil
	}
}
