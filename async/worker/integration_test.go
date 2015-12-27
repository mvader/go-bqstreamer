// +build integration

package worker

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
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

// TestInsertTableToBigQuery test stream inserting a row (given as argument)
// 5 times to BigQuery, and logs the response.
// NOTE this test doesn't check if the inserted rows were inserted correctly,
// it just inserts them.
//
// Usage: 'go test -v -tags=integration -key /path/to/key.json -project projectID -dataset datasetID -table tableID'
func TestInsertTableToBigQuery(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	flushed := make(chan struct{})
	client := http.Client{
		Transport: NewNotifyTransport(
			jwtConfig.Client(oauth2.NoContext).Transport,
			// Notify "flushed" via channel on calling InsertAll().
			func(transport http.RoundTripper, req *http.Request) (*http.Response, error) {
				res, err := transport.RoundTrip(req)
				// NOTE notifying "flushed" after the request has been made.
				flushed <- struct{}{}
				return res, err
			})}

	// Set flush threshold to 5 so flush will happen immediately.
	s, err := New(&client, SetMaxRows(5), SetMaxDelay(1*time.Second), SetSleepBeforeRetry(1*time.Second), SetMaxRetryInsert(3))
	require.NoError(err)

	// Queue two good rows and two invalid rows.
	// Invalid rows should be rejected and trigger a retry insert.
	s.QueueRow(lib.Row{*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 1, "b": "1", "c": "2006-01-02T15:04:01.000000Z"}})
	s.QueueRow(lib.Row{*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": "wrong value type", "b": "2", "c": "2006-01-02T15:04:02.000000Z"}})
	s.QueueRow(lib.Row{*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 3, "b": "3", "c": "2006-01-02T15:04:03.000000Z", "d": "non-existent field name"}})
	s.QueueRow(lib.Row{*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 4, "b": "4", "c": "2006-01-02T15:04:04.000000Z"}})

	// Start the server and wait enough time for the server to flush.
	s.Start()
	select {
	case <-flushed:
	case <-time.After(10 * time.Second):
		assert.Fail("Insert wasn't called fast enough")
	}

	// A retry insert should occur since two documents were invalid.
	select {
	case <-flushed:
	case <-time.After(10 * time.Second):
		assert.Fail("Retry insert wasn't called fast enough")
	}

	t.Log("Waiting for 10 seconds and making sure flush isn't called a third time")
	select {
	case <-flushed:
		require.Fail("Insert was called a third time")
	case <-time.After(10 * time.Second):
	}

	select {
	case <-s.Stop():
	case <-time.After(1 * time.Second):
		assert.Fail("Start() loop didn't stop fast enough")
	}

	// Test BigQuery errors.
	select {
	case err, ok := <-s.ErrorChan:
		if assert.True(ok, "Error channel is closed") {
			assert.EqualError(
				err,
				fmt.Sprintf("%s.%s.%s.row[1]: ", *projectID, *datasetID, *tableID)+
					`invalid in Field:a: `+
					`Could not convert value to integer (bad value or out of range).: `+
					`{"a":"wrong value type","b":"2","c":"2006-01-02T15:04:02.000000Z"}`)
		}
	default:
		require.Fail("Error channel is empty when testing for first error")
	}

	select {
	case err, ok := <-s.ErrorChan:
		if assert.True(ok, "Error channel is closed") {
			assert.EqualError(
				err,
				fmt.Sprintf("%s.%s.%s.row[2]: ", *projectID, *datasetID, *tableID)+
					`invalid in Field:d: `+
					`no such field: `+
					`{"a":3,"b":"3","c":"2006-01-02T15:04:03.000000Z","d":"non-existent field name"}`)
		}
	default:
		require.Fail("Error channel is empty when testing for second error")
	}

	select {
	case err, ok := <-s.ErrorChan:
		if ok {
			assert.Fail("Error channel isn't empty after two expected error messages:", err)
		} else {
			assert.Fail("Error channel is closed after two expected error messages")
		}
	default:
	}
}

// NotifyTransport is a mock http.Transport, and implements http.RoundTripper
// interface.
//
// It notifies via channel that the RoundTripper() function was called,
// then calls and returns the embedded Transport.RoundTripper().
type NotifyTransport struct {
	transport http.RoundTripper
	roundTrip func(http.RoundTripper, *http.Request) (*http.Response, error)
}

func NewNotifyTransport(
	transport http.RoundTripper,
	roundTrip func(http.RoundTripper, *http.Request) (*http.Response, error)) *NotifyTransport {

	return &NotifyTransport{
		transport: transport,
		roundTrip: roundTrip}
}

func (t *NotifyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.roundTrip(t.transport, req)
}
