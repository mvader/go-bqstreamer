package worker

import (
	"log"
	"time"

	"github.com/rounds/go-bqstreamer/lib"
	"golang.org/x/oauth2"
	"google.golang.org/api/bigquery/v2"
)

// This example uses a single Worker.
// A single row is queued, and will be flushed once a time threshold has passed,
// or if the Worker is explicitly closed.
//
// Note starting a Worker is a blocking operation, so it needs to run in its own goroutine.
//
// You should probably use a Streamer, as it provides better concurrency and speed,
// but Worker is there if you need to.
func ExampleWorker() {
	// Init OAuth2/JWT. This is required for authenticating with BigQuery.
	// See the following URLs for more info:
	// https://cloud.google.com/bigquery/authorization
	// https://developers.google.com/console/help/new/#generatingoauth2
	jwtConfig, err := lib.NewJWTConfig("path_to_key.json")
	if err != nil {
		log.Fatalln(err)
	}

	// Init a new Worker.
	s, err := New(
		jwtConfig.Client(oauth2.NoContext), // http.Client authenticated via OAuth2.
		SetMaxRows(500),                    // Amount of rows queued before forcing insert to BigQuery.
		SetMaxDelay(1*time.Second),         // Time to pass between forcing insert to BigQuery.
		SetSleepBeforeRetry(1*time.Second), // Time to wait between failed insert retries.
		SetMaxRetryInsert(10))              // Maximum amount of failed insert retries before discarding rows and moving on.

	if err != nil {
		log.Fatalln(err)
	}

	// Start the Worker.
	// A Worker is blocking, so it needs to be start in its own goroutine.
	go s.Start()
	defer s.Stop()

	// Queue a single row.
	// Insert will happen once "max delay" time has passed,
	// or "max rows" have been queued.
	s.QueueRow(
		lib.Row{
			ProjectID: "project-id",
			DatasetID: "dataset-id",
			TableID:   "table-id",
			Data:      map[string]bigquery.JsonValue{"key": "value"},
		})
}
