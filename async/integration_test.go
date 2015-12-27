// +build integration

package async

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/rounds/go-bqstreamer/lib"
)

var (
	keyPath   = flag.String("key", "", "oauth2 json key path, acquired via https://console.developers.google.com")
	projectID = flag.String("project", "", "bigquery project id")
	datasetID = flag.String("dataset", "", "bigquery dataset id")
	tableID   = flag.String("table", "", "bigquery table id")

	jwtConfig *jwt.Config
)

func init() {
	flag.Parse()

	// Validate custom parameters.
	crash := true
SANITY:
	switch {
	case *keyPath == "":
		fmt.Println("missing key parameter")
	case *projectID == "":
		fmt.Println("missing project parameter")
	case *datasetID == "":
		fmt.Println("missing dataset parameter")
	case *tableID == "":
		fmt.Println("missing table parameter")
	default:
		var err error
		if jwtConfig, err = lib.NewJWTConfig(*keyPath); err != nil {
			fmt.Println(err)
			break SANITY
		}
		return
	}

	if crash {
		flag.Usage()
		os.Exit(2)
	}
}

// TestStreamerInsertTableToBigQuery test stream inserting a row (given as argument)
// 5 times to BigQuery using a Streamer, and logs the response.
// NOTE this test doesn't check if the inserted rows were inserted correctly,
// it just inserts them.
//
// Usage: 'go test -v -tags=integration-streamer -key /path/to/key.json -project projectID -dataset datasetID -table tableID'
func TestStreamerInsertTableToBigQuery(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set flush max delay threshold to 1 second so workers will flush
	// almost immediately.
	s, err := New(jwtConfig, SetNumWorkers(3), SetMaxRows(5), SetMaxDelay(1*time.Second), SetSleepBeforeRetry(1*time.Second), SetMaxRetryInsert(1))
	require.NoError(err)

	// Queue two good rows and two invalid rows.
	// Invalid rows should be rejected and trigger a retry insert.
	s.QueueRow(lib.Row{*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 1, "b": "1", "c": "2006-01-02T15:04:01.000000Z"}})
	s.QueueRow(lib.Row{*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": "wrong value type", "b": "2", "c": "2006-01-02T15:04:02.000000Z"}})
	s.QueueRow(lib.Row{*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 3, "b": "3", "c": "2006-01-02T15:04:03.000000Z", "d": "non-existent field name"}})
	s.QueueRow(lib.Row{*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 4, "b": "4", "c": "2006-01-02T15:04:04.000000Z"}})

	// Start and wait a bit for workers to read inserted rows.
	// Then, stop the Streamer, forcing all workers to flush.
	s.Start()
	time.Sleep(5 * time.Second)
	s.Stop()

	// Log BigQuery errors.
	// NOTE these are not test failures, just responses.
	errors := 0
ErrorHandling:
	for {
		errors++
		select {
		case err, ok := <-s.ErrorChan():
			if errors < 2 {
				require.True(ok)
				assert.NotNil(err)
			} else {
				require.False(ok)
				assert.Nil(err)
				break ErrorHandling
			}
		default:
			assert.Fail("read from error channel blocked")
		}
	}
}
