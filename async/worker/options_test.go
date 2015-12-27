package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rounds/go-bqstreamer/lib"
)

// TestOptions creates a new Worker and tests all options are working.
func TestOptions(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	// Test sending bad arguments.
	w := Worker{}
	assert.EqualError(SetMaxRows(0)(&w), "max rows must be a positive int")
	assert.EqualError(SetMaxRows(-1)(&w), "max rows must be a positive int")
	assert.EqualError(SetMaxDelay(0)(&w), "max delay must be a positive time.Duration")
	assert.EqualError(SetMaxDelay(-1)(&w), "max delay must be a positive time.Duration")
	assert.EqualError(SetSleepBeforeRetry(0)(&w), "sleep before retry must be a positive time.Duration")
	assert.EqualError(SetSleepBeforeRetry(-1)(&w), "sleep before retry must be a positive time.Duration")
	assert.EqualError(SetMaxRetryInsert(-1)(&w), "max retry insert must be a non-negative int")
	assert.EqualError(SetErrorBufferSize(-1)(&w), "error channel size must be a non-negative int")

	assert.EqualError(SetErrorChannel(nil)(&w), "error channel is nil")
	assert.EqualError(SetRowChannel(nil)(&w), "row channel is nil")

	// Test valid arguments
	w = Worker{}
	assert.NoError(SetMaxRows(1)(&w))
	assert.NoError(SetMaxDelay(1 * time.Second)(&w))
	assert.NoError(SetSleepBeforeRetry(2 * time.Second)(&w))
	assert.NoError(SetMaxRetryInsert(2)(&w))
	assert.NoError(SetErrorBufferSize(1)(&w))

	assert.Len(w.Rows, 0)
	assert.Equal(1, cap(w.Rows))
	assert.Len(w.RowChan, 0)
	assert.Equal(1, cap(w.RowChan))
	assert.Equal(w.MaxDelay, 1*time.Second)
	assert.Equal(w.SleepBeforeRetry, 2*time.Second)
	assert.Equal(w.MaxRetryInsert, 2)
	assert.Len(w.ErrorChan, 0)
	assert.Equal(1, cap(w.ErrorChan))

	w = Worker{}
	assert.NoError(SetErrorChannel(make(chan error, 5))(&w))
	assert.NoError(SetRowChannel(make(chan *lib.Row, 5))(&w))
	assert.Len(w.ErrorChan, 0)
	assert.Equal(5, cap(w.ErrorChan))
	assert.Len(w.RowChan, 0)
	assert.Equal(5, cap(w.RowChan))
}
