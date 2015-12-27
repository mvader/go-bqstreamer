package async

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestOptions creates a new Streamer and tests all options are working.
func TestOptions(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	// Test sending bad arguments.
	s := Streamer{}
	assert.EqualError(SetNumWorkers(0)(&s), "number of workers must be a positive int")
	assert.EqualError(SetNumWorkers(-1)(&s), "number of workers must be a positive int")
	assert.EqualError(SetMaxRows(0)(&s), "max rows must be non-negative int")
	assert.EqualError(SetMaxRows(-1)(&s), "max rows must be non-negative int")
	assert.EqualError(SetMaxDelay(0)(&s), "max delay must be a positive time.Duration")
	assert.EqualError(SetMaxDelay(-1)(&s), "max delay must be a positive time.Duration")
	assert.EqualError(SetSleepBeforeRetry(0)(&s), "sleep before retry must be a positive time.Duration")
	assert.EqualError(SetSleepBeforeRetry(-1)(&s), "sleep before retry must be a positive time.Duration")
	assert.EqualError(SetMaxRetryInsert(-1)(&s), "max retry insert must be a non-negative int")
	assert.EqualError(SetErrorBufferSize(0)(&s), "error channel size must be a non-negative int")
	assert.EqualError(SetErrorBufferSize(-1)(&s), "error channel size must be a non-negative int")

	// Test valid arguments
	s = Streamer{}
	assert.NoError(SetNumWorkers(5)(&s))
	assert.NoError(SetMaxRows(1)(&s))
	assert.NoError(SetMaxDelay(1 * time.Second)(&s))
	assert.NoError(SetSleepBeforeRetry(2 * time.Second)(&s))
	assert.NoError(SetMaxRetryInsert(2)(&s))
	assert.NoError(SetErrorBufferSize(1)(&s))

	assert.Equal(s.numWorkers, 5)
	assert.Equal(s.maxRows, 1)
	assert.Len(s.rowChan, s.maxRows*s.numWorkers)
	assert.Equal(1, cap(s.rowChan))
	assert.Equal(s.maxDelay, 1*time.Second)
	assert.Equal(s.sleepBeforeRetry, 2*time.Second)
	assert.Equal(s.maxRetryInsert, 2)
	assert.Len(s.errorChan, 0)
	assert.Equal(1, cap(s.errorChan))
}
