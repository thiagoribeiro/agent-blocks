// Package roomadmin is an internal adapter to the existing Nimlet CLI and HTTP API.
// The operator DSN is present only in this process, never in plugin workers or clients.
package roomadmin

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var ErrInvalid = errors.New("invalid administrative request")
var ErrForbidden = errors.New("administrative request forbidden")

type Error struct{ Status int }

func (e *Error) Error() string { return fmt.Sprintf("room admin operation failed: %d", e.Status) }

type Member struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	State    string `json:"state"`
	Role     string `json:"role"`
	Type     string `json:"type,omitempty"`
}
type Prepared struct {
	RoomID  string   `json:"room_id"`
	Members []Member `json:"members"`
}
type Request struct {
	Action       string   `json:"action"`
	Participants []Member `json:"participants,omitempty"`
	RoomID       string   `json:"room_id"`
	Actor        string   `json:"actor"`
	Key          string   `json:"key,omitempty"`
	Invitees     []string `json:"invitees,omitempty"`
	UserID       string   `json:"user_id,omitempty"`
	Username     string   `json:"username,omitempty"`
}
type Response struct {
	Members  []Member  `json:"members,omitempty"`
	Prepared *Prepared `json:"prepared,omitempty"`
	Member   *Member   `json:"member,omitempty"`
}
type Client struct {
	URL, Token string
	HTTP       *http.Client
}

func (c *Client) Do(ctx context.Context, q Request) (Response, error) {
	b, _ := json.Marshal(q)
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(c.URL, "/")+"/operation", bytes.NewReader(b))
	if err != nil {
		return Response{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	res, err := c.HTTP.Do(req)
	if err != nil {
		return Response{}, fmt.Errorf("room admin unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return Response{}, &Error{Status: res.StatusCode}
	}
	var out Response
	err = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&out)
	return out, err
}

type CLI func(context.Context, ...string) ([]byte, error)
type Server struct {
	Token, Namespace, NimletURL, ASToken, BotToken, BotUsername string
	CLI                                                         CLI
	HTTP                                                        *http.Client
}

var safeName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.@:-]{0,127}$`)
var uuid = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func Executable(binary, namespace string) CLI {
	// Each CLI owns a database pool and a Go process. Bound their aggregate
	// connections/memory when webhooks and journal recovery overlap.
	gate := make(chan struct{}, 1)
	return func(ctx context.Context, args ...string) ([]byte, error) {
		if namespace == "" || strings.HasPrefix(namespace, "-") {
			return nil, fmt.Errorf("namespace required")
		}
		allowed := len(args) >= 2 && (args[0] == "room" && (args[1] == "show" || (args[1] == "member" && len(args) > 2 && (args[2] == "list" || args[2] == "remove" || args[2] == "add"))) || args[0] == "user" && (args[1] == "show" || args[1] == "list"))
		if !allowed {
			return nil, fmt.Errorf("CLI operation forbidden")
		}
		select {
		case gate <- struct{}{}:
			defer func() { <-gate }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		argv := append(append([]string{}, args...), "-n", namespace, "--output", "json")
		cmd := exec.CommandContext(ctx, binary, argv...)
		// The administrative subprocess receives only its own operator credentials,
		// never the adapter's tokens or the plugin's configuration.
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "NIMLET_ADMIN_DB_DSN=" + os.Getenv("NIMLET_ADMIN_DB_DSN"), "NIMLET_ADMIN_OPERATOR=" + os.Getenv("NIMLET_ADMIN_OPERATOR")}
		out, err := cmd.Output()
		if err != nil {
			var exit *exec.ExitError
			if errors.As(err, &exit) && (exit.ExitCode() == 2 || exit.ExitCode() == 3 || exit.ExitCode() == 4) {
				return nil, ErrInvalid
			}
			if errors.As(err, &exit) && exit.ExitCode() == 5 {
				return nil, ErrForbidden
			}
			return nil, fmt.Errorf("Nimlet administrative %s/%s failed", args[0], args[1])
		}
		return out, nil
	}
}

type directoryUser struct {
	ID, Username, Type string
	Disabled           bool
}

// Only the adapter sees this namespace-scoped directory. Batched classification
// avoids a separate CLI process/pool for every member; callers only receive the
// selected room's members. Invitation lookup remains exact and namespace scoped.
func (s *Server) users(ctx context.Context) (map[string]directoryUser, error) {
	users := map[string]directoryUser{}
	after := ""
	for page := 0; page < 100; page++ {
		b, err := s.CLI(ctx, "user", "list", "--limit", "1000", "--after="+after)
		if err != nil {
			return nil, err
		}
		var batch []directoryUser
		if err = json.Unmarshal(b, &batch); err != nil {
			return nil, err
		}
		for _, u := range batch {
			users[u.ID] = u
		}
		if len(batch) < 1000 {
			return users, nil
		}
		next := batch[len(batch)-1].Username
		if next <= after {
			return nil, ErrInvalid
		}
		after = next
	}
	return nil, ErrInvalid
}

func (s *Server) members(ctx context.Context, room string) ([]Member, error) {
	if !uuid.MatchString(room) {
		return nil, ErrInvalid
	}
	b, err := s.CLI(ctx, "room", "member", "list", room)
	if err != nil {
		return nil, err
	}
	var ms []Member
	if err = json.Unmarshal(b, &ms); err != nil {
		return nil, err
	}
	users, err := s.users(ctx)
	if err != nil {
		return nil, err
	}
	for j := range ms {
		if !safeName.MatchString(ms[j].Username) {
			return nil, ErrInvalid
		}
		u, exists := users[ms[j].UserID]
		if !exists || u.Username != ms[j].Username {
			return nil, ErrInvalid
		}
		ms[j].Type = u.Type
	}
	return ms, nil
}
func (s *Server) call(ctx context.Context, path, token string, body any, out any) error {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(s.NimletURL, "/")+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	res, err := s.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("transport unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("transport operation failed: %d", res.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(out)
	}
	return nil
}
func (s *Server) Operation(ctx context.Context, q Request) (Response, error) {
	if s.Namespace == "" || q.Actor == "" {
		return Response{}, ErrInvalid
	}
	ms, err := s.members(ctx, q.RoomID)
	if err != nil {
		return Response{}, err
	}
	joined := false
	for _, m := range ms {
		if m.UserID == q.Actor && m.State == "join" {
			joined = true
		}
	}
	if !joined {
		return Response{}, ErrForbidden
	}
	if q.Action == "prepare" || q.Action == "validate" {
		publisher, consumer := false, false
		for _, m := range ms {
			if m.State == "join" && m.Username == "agent-blocks" {
				publisher = true
			}
			if m.State == "join" && m.Username == s.BotUsername {
				consumer = true
			}
		}
		if !publisher || !consumer {
			return Response{}, ErrInvalid
		}
	}
	switch q.Action {
	case "members":
		return Response{Members: ms}, nil
	case "validate":
		if len(q.Invitees) > 100 {
			return Response{}, ErrInvalid
		}
		selected := map[string]Member{}
		for _, m := range ms {
			if m.Type == "human" && m.State == "join" {
				selected[m.UserID] = m
			}
		}
		for _, name := range q.Invitees {
			if !safeName.MatchString(name) {
				return Response{}, ErrInvalid
			}
			data, err := s.CLI(ctx, "user", "show", name)
			if err != nil {
				return Response{}, err
			}
			var user struct {
				ID, Username, Type string
				Disabled           bool
			}
			if json.Unmarshal(data, &user) != nil || user.Type != "human" || user.Disabled {
				return Response{}, ErrInvalid
			}
			if _, exists := selected[user.ID]; !exists {
				selected[user.ID] = Member{UserID: user.ID, Username: user.Username, Type: "human", State: "invite", Role: "member"}
			}
		}
		members := []Member{}
		for _, m := range selected {
			members = append(members, m)
		}
		return Response{Members: members}, nil
	case "prepare":
		if !uuid.MatchString(q.Key) || len(q.Invitees) > 100 {
			return Response{}, ErrInvalid
		}
		people := map[string]Member{}
		if q.Participants != nil {
			if len(q.Participants) > 1000 {
				return Response{}, ErrInvalid
			}
			for _, m := range q.Participants {
				if !uuid.MatchString(m.UserID) || !safeName.MatchString(m.Username) || m.Type != "human" {
					return Response{}, ErrInvalid
				}
				people[m.UserID] = m
			}
		} else {
			for _, m := range ms {
				if m.State == "join" && m.Type == "human" {
					people[m.UserID] = m
				}
			}
		}
		// Exact namespace-scoped resolution, never a directory search.
		for _, name := range q.Invitees {
			if !safeName.MatchString(name) {
				return Response{}, ErrInvalid
			}
			b, e := s.CLI(ctx, "user", "show", name)
			if e != nil {
				return Response{}, e
			}
			var u struct {
				ID, Username, Type string
				Disabled           bool
			}
			if e = json.Unmarshal(b, &u); e != nil || u.Type != "human" || u.Disabled {
				return Response{}, ErrInvalid
			}
			people[u.ID] = Member{UserID: u.ID, Username: u.Username, Type: u.Type, State: "invite", Role: "member"}
		}
		var room struct {
			ID string `json:"room_id"`
		}
		err = s.call(ctx, "/appservice/agent-blocks/rooms", s.ASToken, map[string]any{"name": "#shared-surface:" + q.Key + "#", "alias": "nimchat:shared:" + q.Key, "visibility": "private"}, &room)
		if err != nil {
			return Response{}, err
		}
		if !uuid.MatchString(room.ID) || !safeName.MatchString(s.BotUsername) {
			return Response{}, ErrInvalid
		}
		// The AS owns transport administration. The normal bot session can issue invitations.
		if _, err = s.CLI(ctx, "room", "member", "add", room.ID, s.BotUsername, "--role", "admin", "--if-not-exists"); err != nil {
			return Response{}, err
		}
		if err = s.call(ctx, "/appservice/agent-blocks/rooms/"+url.PathEscape(room.ID)+"/members", s.ASToken, map[string]any{"user_id": q.Actor}, nil); err != nil {
			return Response{}, err
		}
		dest, err := s.members(ctx, room.ID)
		if err != nil {
			return Response{}, err
		}
		current := map[string]string{}
		for _, m := range dest {
			current[m.UserID] = m.State
		}
		participants := []Member{}
		for _, m := range people {
			if m.UserID == q.Actor {
				m.State = "join"
			} else if current[m.UserID] == "join" {
				m.State = "join"
			} else {
				if err = s.call(ctx, "/rooms/"+url.PathEscape(room.ID)+"/invite", s.BotToken, map[string]any{"user_id": m.UserID}, nil); err != nil {
					return Response{}, err
				}
				m.State = "invite"
			}
			participants = append(participants, m)
		}
		return Response{Prepared: &Prepared{RoomID: room.ID, Members: participants}}, nil
	case "invite":
		if !safeName.MatchString(q.Username) {
			return Response{}, ErrInvalid
		}
		b, e := s.CLI(ctx, "user", "show", q.Username)
		if e != nil {
			return Response{}, e
		}
		var u struct {
			ID, Username, Type string
			Disabled           bool
		}
		if e = json.Unmarshal(b, &u); e != nil || u.Type != "human" || u.Disabled {
			return Response{}, ErrInvalid
		}
		for _, m := range ms {
			if m.UserID == u.ID && (m.State == "join" || m.State == "invite") {
				return Response{Member: &m}, nil
			}
		}
		if err = s.call(ctx, "/rooms/"+url.PathEscape(q.RoomID)+"/invite", s.BotToken, map[string]any{"user_id": u.ID}, nil); err != nil {
			return Response{}, err
		}
		return Response{Member: &Member{UserID: u.ID, Username: u.Username, Type: u.Type, State: "invite", Role: "member"}}, nil
	case "remove":
		if q.UserID == q.Actor {
			return Response{}, ErrInvalid
		}
		for _, m := range ms {
			if m.UserID == q.UserID {
				if m.Type != "human" {
					return Response{}, ErrInvalid
				}
				if m.State == "leave" || m.State == "ban" {
					return Response{}, nil
				}
				_, err = s.CLI(ctx, "room", "member", "remove", q.RoomID, m.Username)
				return Response{}, err
			}
		}
		return Response{}, nil
	}
	return Response{}, ErrInvalid
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" || r.URL.Path != "/operation" {
		http.NotFound(w, r)
		return
	}
	if s.Token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.Token)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	var q Request
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	d.DisallowUnknownFields()
	if d.Decode(&q) != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	out, err := s.Operation(ctx, q)
	if err != nil {
		slog.Warn("administrative operation failed", "action", q.Action, "error", err)
		code := 503
		if errors.Is(err, ErrInvalid) {
			code = 400
		}
		if errors.Is(err, ErrForbidden) {
			code = 403
		}
		http.Error(w, "administrative operation unavailable", code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
