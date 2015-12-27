package async

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"

	"github.com/rounds/go-bqstreamer/async/worker"
	"github.com/rounds/go-bqstreamer/lib"
)

// A Streamer operates multiple background workers via goroutines.
// When a Streamer queues a row, this row is consequently read and queued by
// one of the workers.
// When a time threshold is reached by a worker, or when it queued enough rows,
// it stream inserts to BigQuery using InsertAll().
//
// This thresholds can be set by the option functions.
type Streamer struct {
	// TODO Improve by managing a worker channel, notifying the Streamer when they're
	// ready to read rows, and then letting them read one-by-one
	// (the first will read a chunk and go streaming, then the next one will read and
	// stream, etc.)
	// Right now they're all reading together simultaniously, racing for messages.
	// See here: http://nesv.github.io/golang/2014/02/25/worker-queues-in-go.html

	// BigQuery Worker slice.
	workers []*worker.Worker

	// Channel for sending rows to background Workers.
	rowChan chan *lib.Row

	// Errors are reported to this channel.
	errorChan chan error

	// The following are options, overridable by the option functions:

	// Amount of background workers to use.
	numWorkers int

	// Max amount of rows to queue before flushing to BigQuery.
	maxRows int

	// Max delay between flushes to BigQuery.
	maxDelay time.Duration

	// Sleep delay after a rejected insert and before retry.
	sleepBeforeRetry time.Duration

	// Maximum retry insert attempts for non-rejected row insert errors.
	// e.g. GoogleAPI HTTP errors, generic HTTP errors, etc.
	maxRetryInsert int

	// Buffer size of the error channel.
	errorBufferSize int
}

// New returns a new Streamer.
func New(jwtConfig *jwt.Config, options ...OptionFunc) (*Streamer, error) {
	if jwtConfig == nil {
		return nil, errors.New("jwt.Config is nil")
	}

	// Create a new Streamer, with OAuth2/JWT http.Client constructor function.
	newHTTPClient := func() *http.Client { return jwtConfig.Client(oauth2.NoContext) }
	return newStreamer(newHTTPClient, options...)
}

// newStreamer returns a new Streamer.
//
// It recieves an http.Client constructor, which is used to return an
// authenticated OAuth2/JWT client, or a no-op client for unit tests.
func newStreamer(
	newHTTPClient func() *http.Client,
	options ...OptionFunc) (*Streamer, error) {

	// Override configuration defaults with options if given.
	s := Streamer{}
	for _, option := range options {
		if err := option(&s); err != nil {
			return nil, err
		}
	}

	// Initialize workers and assign them a common row and error channel.
	//
	// NOTE Streamer row length is set as following to avoid filling up
	// in case workers get delayed with insert retries.
	var err error
	s.rowChan = make(chan *lib.Row, s.maxRows*s.numWorkers)
	s.errorChan = make(chan error, s.errorBufferSize)
	s.workers = make([]*worker.Worker, s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		s.workers[i], err = worker.New(
			newHTTPClient(),
			worker.SetRowChannel(s.rowChan),
			worker.SetErrorChannel(s.errorChan),
			worker.SetMaxDelay(s.maxDelay),
			worker.SetSleepBeforeRetry(s.sleepBeforeRetry),
			worker.SetMaxRetryInsert(s.maxRetryInsert))

		if err != nil {
			return nil, err
		}
	}

	return &s, nil
}

// Start starts the workers.
//
// Workers will read queued rows, and insert them to BigQuery once either
// MaxDelay time has passed, or MaxSize rows have been queued by a worker.
//
// Insert errors will be reported to the ErrorChan() channel.
func (s *Streamer) Start() {
	for _, w := range s.workers {
		w.Start()
	}
}

// Stop stops all workers.
// Stop blocks until all workers have inserted their remaining rows to BigQuery
// and stopped.
//
// After all workers have inserted and stopped, the ErrorChan() error channel
// is closed.
func (s *Streamer) Stop() {
	wg := sync.WaitGroup{}
	for _, w := range s.workers {
		wg.Add(1)
		go func(w *worker.Worker) {
			defer wg.Done()
			// Block until worker has stopped.
			<-w.Stop()
		}(w)
	}
	wg.Wait()

	close(s.errorChan)
}

// QueueRow queues a single row,
// which will be read and inserted by one of the workers.
func (s *Streamer) QueueRow(row lib.Row) { s.rowChan <- &row }

// ErrorChan returns a chan error, where insert errors will be sent by
// workers.
//
// Messages to this channel must be handled, otherwise the Streamer will block.
func (s *Streamer) ErrorChan() <-chan error { return s.errorChan }
