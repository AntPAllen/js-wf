package visibility

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"js-wf/identity"
	"js-wf/wf"
)

// Page is a bounded, ordered query result. Next is empty after the last page.
// A cursor belongs to its original status and attribute filter; concurrent
// changes to matching rows can change what later pages contain.
type Page struct {
	Rows []Row  `json:"rows"`
	Next string `json:"next,omitempty"`
}

func decodeCursor(cursor string) (string, string, error) {
	if cursor == "" {
		return "", "", nil
	}
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", fmt.Errorf("invalid visibility cursor: %w", err)
	}
	var parts [2]string
	if err := json.Unmarshal(data, &parts); err != nil {
		return "", "", fmt.Errorf("invalid visibility cursor: %w", err)
	}
	if err := identity.Validate(parts[0], parts[1]); err != nil {
		return "", "", fmt.Errorf("invalid visibility cursor: %w", err)
	}
	return parts[0], parts[1], nil
}

func encodeCursor(row Row) string {
	data, _ := json.Marshal([2]string{row.Type, row.ID})
	return base64.RawURLEncoding.EncodeToString(data)
}

func validatePage(status, cursor string, limit int) (string, string, error) {
	if limit < 1 || limit > 1000 {
		return "", "", fmt.Errorf("visibility page limit must be between 1 and 1000")
	}
	if status != "" && identity.ValidateToken(status) != nil {
		return "", "", fmt.Errorf("invalid status %q", status)
	}
	return decodeCursor(cursor)
}

func finishPage(rows []Row, limit int) Page {
	page := Page{Rows: rows}
	if len(rows) > limit {
		page.Rows = rows[:limit]
		page.Next = encodeCursor(page.Rows[len(page.Rows)-1])
	}
	return page
}

func slicePage(rows []Row, typ, id string, limit int) Page {
	start := 0
	for start < len(rows) && (rows[start].Type < typ || rows[start].Type == typ && rows[start].ID <= id) {
		start++
	}
	end := start + limit + 1
	if end > len(rows) {
		end = len(rows)
	}
	return finishPage(rows[start:end], limit)
}

// ListPage returns up to limit rows in (type, id) order.
func (p *Projection) ListPage(ctx context.Context, status, cursor string, limit int) (Page, error) {
	typ, id, err := validatePage(status, cursor, limit)
	if err != nil {
		return Page{}, err
	}
	if p.postgres != nil {
		rows, err := p.postgres.QueryPage(ctx, status, nil, typ, id, limit+1)
		if err != nil {
			return Page{}, err
		}
		return finishPage(rows, limit), nil
	}
	rows, err := p.List(ctx, status)
	if err != nil {
		return Page{}, err
	}
	return slicePage(rows, typ, id, limit), nil
}

// ListByAttributePage applies an exact attribute match and optional status.
func (p *Projection) ListByAttributePage(ctx context.Context, key, value, status, cursor string, limit int) (Page, error) {
	if err := wf.ValidateSearchAttributes(map[string]string{key: value}); err != nil {
		return Page{}, err
	}
	typ, id, err := validatePage(status, cursor, limit)
	if err != nil {
		return Page{}, err
	}
	if p.postgres != nil {
		rows, err := p.postgres.QueryPage(ctx, status, map[string]string{key: value}, typ, id, limit+1)
		if err != nil {
			return Page{}, err
		}
		return finishPage(rows, limit), nil
	}
	rows, err := p.ListByAttribute(ctx, key, value, status)
	if err != nil {
		return Page{}, err
	}
	return slicePage(rows, typ, id, limit), nil
}
