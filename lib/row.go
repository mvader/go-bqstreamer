package lib

import bigquery "google.golang.org/api/bigquery/v2"

// Row associates a single BigQuery table row to a project, dataset and table.
type Row struct {
	ProjectID,
	DatasetID,
	TableID string
	Data map[string]bigquery.JsonValue
}
