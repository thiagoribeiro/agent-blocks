package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/thiagoribeiro/agent-blocks/internal/plugin"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

// rehashJournal is the one-shot journal upgrade from the legacy stream_pos format (plugin-server
// rehash-journal): it reads the original command events from Nimlet's database
// (NIMLET_DB_DSN) and rewrites this plugin's journal hashes. Run it with the plugin stopped.
func rehashJournal(ctx context.Context) error {
	name, dsn, nimletDSN := os.Getenv("BRIAN_PLUGIN"), os.Getenv("PLUGIN_DB_DSN"), os.Getenv("NIMLET_DB_DSN")
	if name == "" || dsn == "" || nimletDSN == "" {
		return fmt.Errorf("BRIAN_PLUGIN, PLUGIN_DB_DSN and NIMLET_DB_DSN are required")
	}
	store, err := plugin.Open(ctx, dsn, name)
	if err != nil {
		return err
	}
	defer store.Pool.Close()
	ids, err := store.JournalIDs(ctx)
	if err != nil {
		return err
	}
	nimlet, err := pgx.Connect(ctx, nimletDSN)
	if err != nil {
		return err
	}
	defer nimlet.Close(ctx)
	rows, err := nimlet.Query(ctx, `SELECT id, room_id, room_seq, event_type, sender_id, content::text FROM events WHERE id = ANY($1)`, ids)
	if err != nil {
		return err
	}
	events := map[string]protocol.Event{}
	for rows.Next() {
		var e protocol.Event
		var content string
		if err = rows.Scan(&e.EventID, &e.RoomID, &e.RoomSeq, &e.Type, &e.SenderID, &content); err != nil {
			return err
		}
		e.Content = []byte(content)
		events[e.EventID] = e
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rehashed, skipped, err := store.Rehash(ctx, events)
	fmt.Printf("%s: rehashed %d, skipped %d (no source event)\n", name, rehashed, skipped)
	return err
}
