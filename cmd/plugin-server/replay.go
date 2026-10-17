package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/thiagoribeiro/agent-blocks/internal/plugin"
)

// Rewind a single known room explicitly; Runtime still limits recovery to five pages/cycle.
func rewindReplay(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("rewind-replay", flag.ContinueOnError)
	room := flags.String("room", "", "tracked room ID")
	from := flags.Int64("from", 0, "exclusive room_seq cursor")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *room == "" || *from < 0 || flags.NArg() != 0 {
		return fmt.Errorf("--room and a nonnegative --from are required")
	}
	s, err := plugin.Open(ctx, os.Getenv("PLUGIN_DB_DSN"), os.Getenv("BRIAN_PLUGIN"))
	if err != nil {
		return err
	}
	defer s.Pool.Close()
	instances, err := s.Instances(ctx)
	if err != nil {
		return err
	}
	for _, i := range instances {
		if i.RoomID == *room {
			return s.SaveCursor(ctx, "replay:"+*room, strconv.FormatInt(*from, 10))
		}
	}
	return fmt.Errorf("room is not actively tracked by this plugin")
}
