package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

// bearerAuthenticator stores only a digest of one configured credential. In
// simple terms, it answers whether a request has the expected bearer token.
// Precisely, it requires one unambiguous Authorization value and compares fixed-
// size SHA-256 digests in constant time without returning or logging the token.
type bearerAuthenticator struct {
	digest [sha256.Size]byte
}

func newBearerAuthenticator(kind, credential string) (bearerAuthenticator, error) {
	if !validBearerCredential(credential) {
		return bearerAuthenticator{}, errors.New(kind + " bearer credential must be printable, nonblank, and contain no whitespace")
	}
	return bearerAuthenticator{digest: sha256.Sum256([]byte(credential))}, nil
}

func validBearerCredential(credential string) bool {
	if strings.TrimSpace(credential) == "" || strings.ContainsAny(credential, " \t\r\n") {
		return false
	}
	for _, character := range []byte(credential) {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func (auth bearerAuthenticator) authorized(request *http.Request) bool {
	values := request.Header.Values("Authorization")
	if len(values) != 1 {
		return false
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	supplied := sha256.Sum256([]byte(parts[1]))
	return subtle.ConstantTimeCompare(supplied[:], auth.digest[:]) == 1
}
