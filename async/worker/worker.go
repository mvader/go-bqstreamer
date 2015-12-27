package worker

import (
	"net/http"
	"sort"
	"time"

	"github.com/dchest/uniuri"
	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"

	"github.com/rounds/go-bqstreamer/lib"
	"github.com/rounds/go-bqstreamer/lib/errors"
)

// An Inserter is a single asynchronous BigQuery streamer.
// It queues rows and stream inserts to BigQuery in bulk by calling InsertAll().
type Inserter interface {
	QueueRow(lib.Row)
	Start()
	Stop()
	Errors() <-chan error
}

// A Worker is a single async BigQuery streamer (stream inserter),
// queuing rows and stream inserts to BigQuery in bulk by calling InsertAll().
type Worker struct {
	// BigQuery client connection.
	service *bigquery.Service

	// Upon invoking Start(), the worker will fetch rows from this channel,
	// and queue it into an internal rows queue.
	RowChan chan *lib.Row

	// Internal list to queue rows for stream insert.
	Rows []*lib.Row

	// Errors are reported to this channel.
	ErrorChan chan error

	// Max delay between flushes to BigQuery.
	MaxDelay time.Duration

	// Sleep delay after a rejected insert and before retry.
	SleepBeforeRetry time.Duration

	// Maximum retry insert attempts for non-rejected row insert errors.
	// e.g. GoogleAPI HTTP errors, generic HTTP errors, etc.
	MaxRetryInsert int

	// Shutdown channel to stop Start() execution.
	StopChan chan struct{}

	// Used to notify the Start() loop has stopped and returned.
	StoppedChan chan struct{}
}

// New returns a new Worker.
func New(client *http.Client, options ...OptionFunc) (*Worker, error) {
	service, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}

	w := Worker{
		service:          service,
		RowChan:          make(chan *lib.Row, DefaultMaxRows),
		Rows:             make([]*lib.Row, 0, DefaultMaxRows),
		MaxDelay:         DefaultMaxDelay,
		SleepBeforeRetry: DefaultSleepBeforeRetry,
		MaxRetryInsert:   DefaultMaxRetryInsert,
		ErrorChan:        make(chan error, DefaultErrorBufferSize),
		StopChan:         make(chan struct{}),
		// Buffer size of 1 so Start() won't block on notifying it has returned.
		StoppedChan: make(chan struct{}, 1),
	}

	// Override defaults with options if given.
	for _, option := range options {
		if err := option(&w); err != nil {
			return nil, err
		}
	}

	return &w, nil
}

// Start infinitely reads rows from rowChannel and queues them internaly.
// It flushes to BigQuery when the queue is filled (according to maxRows)
// or timer has expired (according to maxDelay).
//
// NOTE the read-insert-flush loop never stops, so this function is
// executed in a goroutine, and stopped via calling Stop().
func (w *Worker) Start() {
	go func(w *Worker) {
		// Notify on return.
		defer func(stopped chan<- struct{}) { close(stopped) }(w.StoppedChan)

		for {
			// Flush and reset timer when one of the following signals (channels) fire:
			select {
			case <-w.StopChan:
				w.Flush()
				return
			case <-time.After(w.MaxDelay):
				w.Flush()
			case r := <-w.RowChan:
				// Insert row to queue.
				w.Rows = append(w.Rows, r)

				// Don't flush if slice isn't full.
				if len(w.Rows) < cap(w.Rows) {
					continue
				}

				w.Flush()
			}
		}
	}(w)
}

// Stop closes stop channel, causing Start()'s infinite loop to stop.
// It returns a notification channel, which will be closed once the Start()
// loop has returned.
func (w *Worker) Stop() <-chan struct{} {
	// Notify Start() loop to return.
	close(w.StopChan)
	return w.StoppedChan
}

// Flush streams all queued rows to BigQuery and resets rows queue by
// creating a new queue.
//
// This function is assigned to Worker.flush member.
// It is overridable so we can test Worker without actually flushing
// to BigQuery.
//
// TODO We should use a mutex to lock the Worker object,
// otherwise if the Worker is running in another goroutine it can call this in parallel.
func (w *Worker) Flush() {
	w.InsertAll()

	// Reset rows queue.
	w.Rows = w.Rows[:0]
}

// QueueRow sends a single row to the row channel,
// which will be queued and inserted in bulk with other queued rows.
func (w *Worker) QueueRow(row lib.Row) { w.RowChan <- &row }

// InsertAll inserts all rows from all tables to BigQuery.
// Each table is inserted separately, according to BigQuery's requirements.
// Insert errors are reported to the error channel.
func (w *Worker) InsertAll() {
	// Sort rows by project->dataset->table
	// Necessary because each InsertAll() has to be for a single table.
	ps := Projects{}
	for _, r := range w.Rows {
		p, d, t := r.ProjectID, r.DatasetID, r.TableID

		// Create project, dataset and table if uninitalized.
		CreateTableIfNotExists(ps, p, d, t)

		// Append row to table,
		// and generate random row ID of 16 character length, for de-duplication purposes.
		ps[p][d][t] = append(ps[p][d][t], &bigquery.TableDataInsertAllRequestRows{
			InsertId: uniuri.NewLen(16),
			Json:     r.Data,
		})
	}

	// Stream insert each table to BigQuery.
	for pID, p := range ps {
		for dID, d := range p {
			for tID := range d {
				// Insert to a single table in bulk, and retry insert on certain errors.
				// Keep on retrying until successful.
				numRetries := 0

			FilterRows:
				for {
					numRetries++
					switch {
					case numRetries > w.MaxRetryInsert:
						w.ErrorChan <- errors.NewTooManyFailedInsertRetriesError(numRetries, pID, dID, tID)
						break FilterRows
					case len(d[tID]) == 0:
						w.ErrorChan <- errors.NewAllRowsRejectedError(pID, dID, tID)
						break FilterRows
					}

					responses, err := w.InsertTable(pID, dID, tID, d[tID])

					// Retry on certain HTTP errors.
					if w.shouldRetryInsertAfterError(err) {
						// Retry after HTTP errors usually mean to retry after a certain pause.
						// See the following link for more info:
						// https://cloud.google.com/bigquery/troubleshooting-errors
						time.Sleep(w.SleepBeforeRetry)
						continue
					}

					// Retry if insert was rejected due to bad rows.
					// Occurence of bad rows do not count against retries,
					// as this means we're trying to insert bad data to BigQuery.
					rejectedRows := w.filterRejectedRows(responses, pID, dID, tID, d)
					if len(rejectedRows) > 0 {
						numRetries--
						continue
					}

					// If we reached here it means insert was successful,
					// so retry isn't necessary.
					// Thus, break from the "retry insert" loop.
					break
				}
			}
		}
	}
}

// InsertTable inserts a single table to BigQuery using BigQuery's InsertAll request.
//
// This function is assigned to Worker.insertTable member.
// It is overridable so we can test Worker without actually inserting anything to BigQuery.
func (w *Worker) InsertTable(projectID, datasetID, tableID string, t Table) (
	*bigquery.TableDataInsertAllResponse, error) {
	// TODO might be better to cache table services somehow, instead of re-creating them on every flush.
	return bigquery.NewTabledataService(w.service).
		InsertAll(
		projectID,
		datasetID,
		tableID,
		&bigquery.TableDataInsertAllRequest{
			Kind: "bigquery#tableDataInsertAllRequest",
			Rows: t}).
		Do()
}

// shouldRetryInsertAfterError checks for insert HTTP response errors,
// and returns true if insert should be retried.
// See the following url for more info:
// https://cloud.google.com/bigquery/troubleshooting-errors
func (w *Worker) shouldRetryInsertAfterError(err error) (shouldRetry bool) {
	shouldRetry = false

	if err != nil {
		// Retry on GoogleAPI HTTP server error (500, 503).
		if gerr, ok := err.(*googleapi.Error); ok {
			switch gerr.Code {
			case 500, 503:
				shouldRetry = true
			}
		}

		// Log and don't retry for any other response codes,
		// or if not a Google API response at all.
		w.ErrorChan <- err
	}

	return
}

// filterRejectedRows checks for per-row responses,
// removes rejected rows given table, and returns index slice of rows removed.
//
// Rows are rejected if BigQuery insert response marked them as any string
// other than "stopped".  See the following url for further info:
// https://cloud.google.com/bigquery/streaming-data-into-bigquery#troubleshooting
func (w *Worker) filterRejectedRows(
	responses *bigquery.TableDataInsertAllResponse,
	pID, dID, tID string,
	d Dataset) (rowsToFilter []int64) {

	// Go through all rows and rows' errors, and remove rejected (bad) rows.
	if responses != nil {
		for _, rowErrors := range responses.InsertErrors {
			// We use a sanity switch to make sure we don't append
			// the same row to be deleted more than once.
			filter := false

			// Each row can have several errors.
			// Go through each of these, and remove row if one of these errors != "stopped" or "timeout".
			// Also log all non-"stopped" errors on the fly.
			for _, rowErrorPtr := range rowErrors.Errors {
				rowError := *rowErrorPtr

				// Mark invalid rows to be deleted.
				switch rowError.Reason {
				// Do nothing for these types of error reason.
				case "stopped", "timeout":

				// Filter and log everything else.
				default:
					if !filter {
						rowsToFilter = append(rowsToFilter, rowErrors.Index)
						filter = true
					}

					// Log all errors besides "stopped" ones.
					w.ErrorChan <- errors.NewRowError(rowError, rowErrors.Index, pID, dID, tID, d[tID][rowErrors.Index].Json)
				}
			}
		}
	}

	// Remove accumulated rejected rows from table (if there were any).
	if len(rowsToFilter) > 0 {
		// Replace modified table instead of original.
		// This is necessary because original table's slice has a different len().
		//
		// XXX is this ok?
		d[tID] = w.filterRowsFromTable(rowsToFilter, d[tID])
	}

	return
}

// "sort"-compliant int64 class. Used for sorting in the functino below.
// This is necessary because for some reason package sort doesn't support sorting int64 slices.
type int64Slice []int64

func (a int64Slice) Len() int           { return len(a) }
func (a int64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64Slice) Less(i, j int) bool { return a[i] < a[j] }

// filterRowsFromTable removes rows by given indexes from given table,
// and returns the filtered table.
// Table filtering is done in place, thus the original table is also changed.
//
// The table is returned in addition to mutating the given table argument for idiom's sake.
func (w *Worker) filterRowsFromTable(indexes []int64, t Table) Table {
	// Deletion is done in-place, so we copy & sort given indexes,
	// then delete in reverse order. Reverse order is necessary for not
	// breaking the order of elements to remove in the slice.
	//
	// Create a copy of given index slice in order to not modify the outer index slice.
	is := append([]int64(nil), indexes...)
	sort.Sort(sort.Reverse(int64Slice(is)))

	for _, i := range is {
		// Garbage collect old row,
		// and switch its place with with last row of slice.
		t[i], t = t[len(t)-1], t[:len(t)-1]
	}

	// Return the same table.
	return t
}
