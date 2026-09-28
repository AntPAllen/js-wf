package visibility

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// PostgresStore is the large-deployment visibility sink. It stores the same
// Row JSON returned by the KV projection while indexing status and attributes.
// Run one projection writer for this table at a time.
type PostgresStore struct{ DB *sql.DB }

func newGeneration() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}

func (s *PostgresStore) Init(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return errors.New("nil PostgreSQL database")
	}
	statements := []string{`CREATE TABLE IF NOT EXISTS wf_visibility (
  type text NOT NULL,
  id text NOT NULL,
  status text NOT NULL,
  attributes jsonb NOT NULL,
  row_data jsonb NOT NULL,
  generation text NOT NULL,
  PRIMARY KEY (type, id)
)`,
		`CREATE INDEX IF NOT EXISTS wf_visibility_status_idx ON wf_visibility (status, type, id)`,
		`CREATE INDEX IF NOT EXISTS wf_visibility_attributes_idx ON wf_visibility USING gin (attributes jsonb_path_ops)`}
	for _, statement := range statements {
		if _, err := s.DB.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) Put(ctx context.Context, row Row, generation string) error {
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	attributes := row.Attributes
	if attributes == nil {
		attributes = map[string]string{}
	}
	attributeData, err := json.Marshal(attributes)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `
INSERT INTO wf_visibility (type, id, status, attributes, row_data, generation)
VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
ON CONFLICT (type, id) DO UPDATE SET
  status = EXCLUDED.status,
  attributes = EXCLUDED.attributes,
  row_data = EXCLUDED.row_data,
  generation = EXCLUDED.generation`, row.Type, row.ID, row.Status, string(attributeData), string(data), generation)
	return err
}

func (s *PostgresStore) Delete(ctx context.Context, typ, id string) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM wf_visibility WHERE type=$1 AND id=$2`, typ, id)
	return err
}

// DeleteOtherGenerations runs only after every source invocation was read and
// written successfully. An interrupted rebuild leaves existing rows intact.
func (s *PostgresStore) DeleteOtherGenerations(ctx context.Context, generation string) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM wf_visibility WHERE generation <> $1`, generation)
	return err
}

func (s *PostgresStore) Get(ctx context.Context, typ, id string) (Row, error) {
	var data []byte
	err := s.DB.QueryRowContext(ctx, `SELECT row_data FROM wf_visibility WHERE type=$1 AND id=$2`, typ, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return Row{}, ErrNotFound
	}
	if err != nil {
		return Row{}, err
	}
	var row Row
	err = json.Unmarshal(data, &row)
	return row, err
}

func (s *PostgresStore) List(ctx context.Context, status string) ([]Row, error) {
	query := `SELECT row_data FROM wf_visibility ORDER BY type, id`
	var args []any
	if status != "" {
		query = `SELECT row_data FROM wf_visibility WHERE status=$1 ORDER BY type, id`
		args = append(args, status)
	}
	return s.queryRows(ctx, query, args...)
}

func (s *PostgresStore) ListByAttribute(ctx context.Context, key, value, status string) ([]Row, error) {
	filter, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		return nil, err
	}
	query := `SELECT row_data FROM wf_visibility WHERE attributes @> $1::jsonb ORDER BY type, id`
	args := []any{string(filter)}
	if status != "" {
		query = `SELECT row_data FROM wf_visibility WHERE attributes @> $1::jsonb AND status=$2 ORDER BY type, id`
		args = append(args, status)
	}
	return s.queryRows(ctx, query, args...)
}

func (s *PostgresStore) queryRows(ctx context.Context, query string, args ...any) ([]Row, error) {
	result, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	rows := make([]Row, 0)
	for result.Next() {
		var data []byte
		if err := result.Scan(&data); err != nil {
			return nil, err
		}
		var row Row
		if err := json.Unmarshal(data, &row); err != nil {
			return nil, fmt.Errorf("decode PostgreSQL visibility row: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, result.Err()
}
