// Here be helper BigQuery data types.

package worker

import bigquery "google.golang.org/api/bigquery/v2"

type Projects map[string]Project
type Project map[string]Dataset
type Dataset map[string]Table
type Table []*bigquery.TableDataInsertAllRequestRows

// CreateTableIfNotExists initializes given project, dataset, and table
// in project map if they haven't been initialized yet.
func CreateTableIfNotExists(ps map[string]Project, p, d, t string) {
	// Create table's project if non-existent.
	if _, ok := ps[p]; !ok {
		ps[p] = Project{}
	}

	// Create table's dataset if non-existent.
	if _, ok := ps[p][d]; !ok {
		ps[p][d] = Dataset{}
	}

	// Create table if non-existent.
	if _, ok := ps[p][d][t]; !ok {
		ps[p][d][t] = Table{}
	}
}
