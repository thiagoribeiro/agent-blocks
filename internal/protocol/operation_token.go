package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// OperationToken derives a bearer token that only authorizes publishing and
// completing one operation. It is what the plugin hands to the workflow engine,
// so the plugin's own token is never stored in workflow definitions.
func OperationToken(pluginToken, operationID string) string {
	m := hmac.New(sha256.New, []byte(pluginToken))
	m.Write([]byte("operation:" + operationID))
	return hex.EncodeToString(m.Sum(nil))
}
