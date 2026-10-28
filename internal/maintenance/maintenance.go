// Package maintenance removes data that has passed its retention window.
package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Result struct {
	Positions int64
	Sessions  int64
	Imports   int64
}

// Run deletes position history older than retentionDays, sessions expired
// more than seven days ago, and import jobs older than 30 days. Positions
// are deleted in batches so a large table does not hold one long lock.
func Run(ctx context.Context, db *sql.DB, retentionDays, batch int) (Result, error) {
	var out Result
	cutoff := time.Now().AddDate(0, 0, -retentionDays)

	for {
		res, err := db.ExecContext(ctx, `
			DELETE FROM position_history
			WHERE id IN (
				SELECT id FROM position_history
				WHERE recorded_at < $1
				LIMIT $2
			)
		`, cutoff, batch)
		if err != nil {
			return out, fmt.Errorf("delete positions: %w", err)
		}
		n, _ := res.RowsAffected()
		out.Positions += n
		if n == 0 {
			break
		}
		if n < int64(batch) {
			break
		}
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	res, err := db.ExecContext(ctx, `
		DELETE FROM sessions WHERE expires_at < now() - interval '7 days'
	`)
	if err != nil {
		return out, fmt.Errorf("delete sessions: %w", err)
	}
	out.Sessions, _ = res.RowsAffected()

	res, err = db.ExecContext(ctx, `
		DELETE FROM import_jobs WHERE created_at < now() - interval '30 days'
	`)
	if err != nil {
		return out, fmt.Errorf("delete import jobs: %w", err)
	}
	out.Imports, _ = res.RowsAffected()

	return out, nil
}
