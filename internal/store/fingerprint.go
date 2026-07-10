// Fingerprint computation for the analytics store.
//
// Two fingerprints are computed and stored alongside every Issue:
//
//   - bugFingerprint: identity of a bug across all tenants. Used to
//     answer "what's the most common bug in the fleet?".
//     Inputs: service_name, resolved_repo, file, line, category.
//
//   - tenantFingerprint: identity of "this bug at this tenant".
//     Used to answer "has this onprem been stuck on the same bug
//     for N days?".
//     Inputs: account_id, flow_id, ophid, service_name, file, line,
//     category.
//
// Both are SHA-256 hex digests of a length-prefixed, lower-cased
// tuple. Empty / missing fields are encoded as the literal "-" so two
// rows with the same observed fields hash identically regardless of
// which tenant dimensions happen to be unset.
package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
)

// bugFingerprint hashes (service_name | resolved_repo | file | line | category).
// All inputs are lowercased; missing fields are encoded as "-".
func bugFingerprint(serviceName, resolvedRepo, file string, line int, category string) string {
	return hashParts(
		normalize(serviceName),
		normalize(resolvedRepo),
		normalize(file),
		normalizeInt(line),
		normalize(category),
	)
}

// BugFingerprint is the exported version for cross-package use (e.g. kafkaio).
func BugFingerprint(serviceName, resolvedRepo, file string, line int, category string) string {
	return bugFingerprint(serviceName, resolvedRepo, file, line, category)
}

// tenantFingerprint hashes the bug identity scoped to one tenant tuple.
// Inputs are (account_id | flow_id | ophid | service_name | file | line | category).
// All inputs are lowercased; missing fields are encoded as "-".
func tenantFingerprint(accountID, flowID, ophID, serviceName, file string, line int, category string) string {
	return hashParts(
		normalize(accountID),
		normalize(flowID),
		normalize(ophID),
		normalize(serviceName),
		normalize(file),
		normalizeInt(line),
		normalize(category),
	)
}

// hashParts is collision-resistant across part boundaries: every part
// is written as a 4-byte big-endian length prefix followed by the
// bytes. Two distinct input tuples can never produce the same digest
// regardless of part content (no pipe / null collisions).
func hashParts(parts ...string) string {
	h := sha256.New()
	var lenBuf [4]byte
	for _, p := range parts {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(p)))
		_, _ = h.Write(lenBuf[:])
		_, _ = h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func normalize(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "-"
	}
	return s
}

func normalizeInt(n int) string {
	if n <= 0 {
		return "-"
	}
	return strconv.Itoa(n)
}
