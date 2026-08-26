package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

func fingerprintSubmission(submission Submission) ([sha256.Size]byte, error) {
	canonicalPayload, err := canonicalJSON(submission.Payload)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	var encoded bytes.Buffer
	writeFingerprintValue(&encoded, "task", string(submission.TaskType))
	writeFingerprintValue(&encoded, "payload", string(canonicalPayload))
	if submission.MaxAttempts == nil {
		writeFingerprintValue(&encoded, "max_attempts", "absent")
	} else {
		writeFingerprintValue(&encoded, "max_attempts", "present:"+strconv.Itoa(*submission.MaxAttempts))
	}
	if submission.AvailableAt == nil {
		writeFingerprintValue(&encoded, "available_at", "absent")
	} else {
		writeFingerprintValue(&encoded, "available_at", "present:"+submission.AvailableAt.UTC().Format("2006-01-02T15:04:05.999999999Z"))
	}
	return sha256.Sum256(encoded.Bytes()), nil
}

func writeFingerprintValue(buffer *bytes.Buffer, name, value string) {
	buffer.WriteString(name)
	buffer.WriteByte(':')
	buffer.WriteString(strconv.Itoa(len(value)))
	buffer.WriteByte(':')
	buffer.WriteString(value)
	buffer.WriteByte(';')
}

// canonicalJSON emits valid JSON with sorted object keys.
// Number normalization makes equivalent forms such as 1, 1.0, and 1e0 equal.
func canonicalJSON(payload json.RawMessage) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("payload must contain one JSON value")
	}
	var canonical bytes.Buffer
	if err := writeCanonicalJSON(&canonical, value); err != nil {
		return nil, err
	}
	return canonical.Bytes(), nil
}

func writeCanonicalJSON(buffer *bytes.Buffer, value any) error {
	switch typed := value.(type) {
	case nil:
		buffer.WriteString("null")
	case bool:
		if typed {
			buffer.WriteString("true")
		} else {
			buffer.WriteString("false")
		}
	case string:
		encoded, _ := json.Marshal(typed)
		buffer.Write(encoded)
	case json.Number:
		normalized, err := normalizeJSONNumber(string(typed))
		if err != nil {
			return err
		}
		buffer.WriteString(normalized)
	case []any:
		buffer.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				buffer.WriteByte(',')
			}
			if err := writeCanonicalJSON(buffer, item); err != nil {
				return err
			}
		}
		buffer.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		buffer.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				buffer.WriteByte(',')
			}
			encodedKey, _ := json.Marshal(key)
			buffer.Write(encodedKey)
			buffer.WriteByte(':')
			if err := writeCanonicalJSON(buffer, typed[key]); err != nil {
				return err
			}
		}
		buffer.WriteByte('}')
	default:
		return errors.New("unsupported JSON value")
	}
	return nil
}

func normalizeJSONNumber(value string) (string, error) {
	negative := strings.HasPrefix(value, "-")
	if negative {
		value = value[1:]
	}
	parts := strings.FieldsFunc(value, func(r rune) bool { return r == 'e' || r == 'E' })
	if len(parts) > 2 {
		return "", errors.New("invalid JSON number")
	}
	exponent := new(big.Int)
	if len(parts) == 2 {
		if _, ok := exponent.SetString(parts[1], 10); !ok {
			return "", errors.New("invalid JSON number exponent")
		}
	}
	mantissa := strings.Split(parts[0], ".")
	if len(mantissa) > 2 {
		return "", errors.New("invalid JSON number")
	}
	digits := strings.Join(mantissa, "")
	if len(mantissa) == 2 {
		exponent.Sub(exponent, big.NewInt(int64(len(mantissa[1]))))
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return "0e0", nil
	}
	trimmed := strings.TrimRight(digits, "0")
	exponent.Add(exponent, big.NewInt(int64(len(digits)-len(trimmed))))
	if negative {
		trimmed = "-" + trimmed
	}
	return trimmed + "e" + exponent.String(), nil
}
