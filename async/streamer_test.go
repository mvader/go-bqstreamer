package async

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/rounds/go-bqstreamer/async/worker"
	"github.com/rounds/go-bqstreamer/lib"
)

// TestStreamer tests creating a new Streamer.
func TestNew(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Test giving bad arguments.
	var err error
	_, err = New(nil)
	assert.EqualError(err, "jwt.Config is nil")
	_, err = newStreamer(nil, SetNumWorkers(0), SetMaxRows(10), SetMaxDelay(1), SetSleepBeforeRetry(1), SetMaxRetryInsert(10), SetErrorBufferSize(10))
	assert.EqualError(err, "number of workers must be a positive int")

	// Test valid arguments.
	var s *Streamer
	s, err = newStreamer(func() *http.Client { return &http.Client{} }, SetNumWorkers(50), SetMaxRows(10), SetMaxDelay(1*time.Second), SetSleepBeforeRetry(1*time.Second), SetMaxRetryInsert(10), SetErrorBufferSize(10))
	assert.NoError(err)
	assert.Empty(s.rowChan)
	assert.Equal(50*10, cap(s.rowChan))
	assert.NotNil(s.errorChan)
	assert.Empty(s.errorChan)
	assert.Equal(10, cap(s.errorChan))

	// ErrorChan() should return <-s.ErrorChan (receive only).
	assert.Equal(s.errorChan, s.ErrorChan())

	for _, w := range s.workers {
		require.NotNil(w)
		require.Equal(w.ErrorChan, s.errorChan)
		require.Equal(w.RowChan, s.rowChan)
		require.Len(w.Rows, 10)
		require.Equal(1*time.Second, w.MaxDelay)
	}
}

// TestStopStreamer tests calling Stop() stops all workers.
func TestStopStreamer(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Using nopClient like in TestNewSteramer().
	s, err := newStreamer(func() *http.Client { return &http.Client{} }, SetNumWorkers(50), SetMaxRows(10), SetMaxDelay(1*time.Second), SetSleepBeforeRetry(1*time.Second), SetMaxRetryInsert(10))
	require.NoError(err)

	// Start then stop workers, and test if they were stopped.
	s.Start()
	s.Stop()

	// Test if stop notification was sent to every worker.
	for i, w := range s.workers {
		select {
		case _, ok := <-w.StopChan:
			// A closed stop channel indicates the stop notification was sent.
			require.False(ok, fmt.Sprintf("worker %d StopChan is open", i))
		case <-time.After(1 * time.Millisecond):
			require.Fail("Worker %d wasn't stopped fast enough", i)
		}
	}

	// Test if the worker has stopped.
	for i, w := range s.workers {
		select {
		case _, ok := <-w.StoppedChan:
			// A closed stopped channel indicates the worker has stopped.
			require.False(ok, fmt.Sprintf("worker %d StoppedChan is open", i))
		case <-time.After(1 * time.Millisecond):
			require.Fail("Worker %d wasn't stopped fast enough", i)
		}
	}

	// errorChan should be closed after all workers have been stopped.
	err, ok := <-s.ErrorChan()
	assert.Nil(err)
	assert.False(ok)
}

// TestQueueRowStreamer queues 4 rows to 2 tables, 2 datasets,
// and 2 projects (total 4 rows) and tests if they were queued by the workers.
func TestQueueRowStreamer(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Track inserted rows.
	ps := worker.Projects{}

	// Create mock http.Clients to be used by workers.
	client := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			// Add all table rows to a local projects-datasets-table map,
			// mocking rows that were inserted to BigQuery, which we will test against.
			for _, tr := range tableReq.Rows {
				assert.NotNil(tr)

				// Mock "insert row" to table: Create project, dataset and table
				// if uninitalized.
				worker.CreateTableIfNotExists(ps, pID, dID, tID)
				ps[pID][dID][tID] = append(ps[pID][dID][tID], &bigquery.TableDataInsertAllRequestRows{Json: tr.Json})
			}

			res := http.Response{
				Header:     make(http.Header),
				Request:    req,
				StatusCode: 200,
				// Empty JSON body, meaning "no errors".
				Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}

			return &res, nil
		})}

	s, err := newStreamer(func() *http.Client { return &client }, SetNumWorkers(5), SetMaxRows(10), SetMaxDelay(1*time.Second), SetSleepBeforeRetry(1*time.Second), SetMaxRetryInsert(10))
	require.NoError(err)

	// Queue rows to 2 tables, 2 datasets, and 2 projects (total 4 rows).
	s.QueueRow(lib.Row{"p1", "d1", "t1", map[string]bigquery.JsonValue{"test_key": "test_value"}})
	s.QueueRow(lib.Row{"p1", "d1", "t2", map[string]bigquery.JsonValue{"test_key": "test_value"}})
	s.QueueRow(lib.Row{"p1", "d2", "t1", map[string]bigquery.JsonValue{"test_key": "test_value"}})
	s.QueueRow(lib.Row{"p2", "d1", "t1", map[string]bigquery.JsonValue{"test_key": "test_value"}})

	// Start and stop Streamer to force flush.
	s.Start()
	s.Stop()

	// Test for inserted rows by comparing tracked Projects.
	assert.Len(ps, 2)

	if p1, ok := ps["p1"]; assert.True(ok) {
		assert.Len(p1, 2)

		if d1, ok := p1["d1"]; assert.True(ok) {
			assert.Len(d1, 2)

			if t1, ok := d1["t1"]; assert.True(ok) {
				if assert.Len(t1, 1) {
					assert.Equal(map[string]bigquery.JsonValue{"test_key": "test_value"}, t1[0].Json)
				}
			}
			if t2, ok := d1["t2"]; assert.True(ok) {
				if assert.Len(t2, 1) {
					assert.Equal(map[string]bigquery.JsonValue{"test_key": "test_value"}, t2[0].Json)
				}
			}
		}
		if d2, ok := p1["d2"]; assert.True(ok) {
			assert.Len(d2, 1)

			if t1, ok := d2["t1"]; assert.True(ok) {
				if assert.Len(t1, 1) {
					assert.Equal(map[string]bigquery.JsonValue{"test_key": "test_value"}, t1[0].Json)
				}
			}
		}
	}
	if p2, ok := ps["p2"]; assert.True(ok) {
		assert.Len(p2, 1)

		if d1, ok := p2["d1"]; assert.True(ok) {
			assert.Len(d1, 1)

			if t1, ok := d1["t1"]; assert.True(ok) {
				if assert.Len(t1, 1) {
					assert.Equal(map[string]bigquery.JsonValue{"test_key": "test_value"}, t1[0].Json)
				}
			}
		}
	}
}

// getInsertMetadata is a helper function that fetches the project, dataset,
// and table IDs from a url string.
func getInsertMetadata(url string) (projectID, datasetID, tableID string) {
	re := regexp.MustCompile(`bigquery/v2/projects/(?P<projectID>.+?)/datasets/(?P<datasetID>.+?)/tables/(?P<tableId>.+?)/insertAll`)
	res := re.FindAllStringSubmatch(url, -1)
	p := res[0][1]
	d := res[0][2]
	t := res[0][3]
	return p, d, t
}

// transport is a mock http.Transport, and implements http.RoundTripper
// interface.
//
// It is used for mocking BigQuery responses via bigquery.Service.
type transport struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func newTransport(roundTrip func(*http.Request) (*http.Response, error)) *transport {
	return &transport{roundTrip}
}
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) { return t.roundTrip(req) }
