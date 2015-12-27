package async

import (
	"log"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/rounds/go-bqstreamer/lib"
)

// This example initializes a Streamer, sets up an error handling goroutine,
// and queues a single row.
// An insert to BigQuery will once one of the following happens:
//  - MaxDelay time has passed,
//  - MaxRows rows have been queued,
//  - Streamer has been stopped.
func ExampleStreamer() {
	// Init OAuth2/JWT. This is required for authenticating with BigQuery.
	// See the following links for further info:
	//
	// https://cloud.google.com/bigquery/authorization
	// https://developers.google.com/console/help/new/#generatingoauth2
	jwtConfig, err := lib.NewJWTConfig("path_to_key.json")
	if err != nil {
		log.Fatalln(err)
	}

	// Init a new Streamer.
	s, err := New(
		jwtConfig,
		SetMaxRows(500),                    // Amount of rows queued before forcing insert to BigQuery.
		SetMaxDelay(1*time.Second),         // Time to pass between inserts to BigQuery.
		SetSleepBeforeRetry(1*time.Second), // Time to wait between failed insert retries.
		SetMaxRetryInsert(10),              // Maximum amount of retrying a failed insert  before discarding its rows and moving on.
		SetErrorBufferSize(50))             // Error channel size. Not emptying this channel will cause the Streamer to block.

	if err != nil {
		log.Fatalln(err)
	}

	// Start Streamer.
	// Start() starts the background workers and returns.
	s.Start()
	// Stop() blocks until all workers have flushed their remaining rows to
	// BigQuery and stopped.
	defer s.Stop()

	// Worker errors are send to Streamer.ErrorChan() channel.
	// The following executes an error logging goroutine,
	// where errors are received from this channel.
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case err := <-s.ErrorChan():
				log.Println(err)
			}
		}
	}()

	// Queue a single row.
	// Insert will happen once one of the following happens:
	// MaxDelay time has passed,
	// MaxRows rows have been queued,
	// Streamer has been stopped.
	s.QueueRow(
		lib.Row{
			ProjectID: "my-project",
			DatasetID: "my-dataset",
			TableID:   "my-table",
			Data:      map[string]bigquery.JsonValue{"key": "value"}})
}
