// Package httpx holds the small HTTP and environment helpers shared by the
// router and plugin binaries.
package httpx

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"os"
)

// MaxBody bounds JSON request bodies.
const MaxBody = 256 << 10

// Env returns the environment variable k, or d when it is unset or empty.
func Env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// WriteJSON writes v as a JSON response with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Decode strictly decodes a bounded JSON body into v. On failure it writes a
// 400 response and returns false.
func Decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBody)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return false
	}
	return true
}

// Authorized reports whether r carries "Bearer <token>". An empty token never matches.
func Authorized(r *http.Request, token string) bool {
	return token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) == 1
}
