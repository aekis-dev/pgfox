package pgfox

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// PostgreSQL protocol constants
const (
	StartupMessageType              = 0
	AuthenticationOK                = 0
	AuthenticationCleartextPassword = 3
	AuthenticationMD5               = 5
	AuthenticationSASL              = 10
	AuthenticationSASLContinue      = 11
	AuthenticationSASLFinal         = 12
	ReadyForQuery                   = 'Z'
	Query                           = 'Q'
	Parse                           = 'P'
	Bind                            = 'B'
	Execute                         = 'E'
	Sync                            = 'S'
	RowDescription                  = 'T'
	DataRow                         = 'D'
	CommandComplete                 = 'C'
	ErrorResponse                   = 'E'
	ParameterStatus                 = 'S'
	BackendKeyData                  = 'K'
	NotificationResponse            = 'A'
	Terminate                       = 'X'
	CopyInResponse                  = 'G'
	CopyOutResponse                 = 'H'
	CopyData                        = 'd'
	CopyDone                        = 'c'

	// Protocol versions and special requests
	ProtocolVersion30 = 196608   // PostgreSQL protocol version 3.0
	SSLRequestCode    = 80877103 // SSL request magic number
	CancelRequestCode = 80877102 // Cancel request magic number
)

// NotificationMessage represents a PostgreSQL notification
type NotificationMessage struct {
	ProcessID int32
	Channel   string
	Payload   string
}

// StartupMessage contains parsed startup message parameters
type StartupMessage struct {
	ProtocolVersion int32
	Parameters      map[string]string
	User            string
	Database        string
}

// Utility functions for reading/writing PostgreSQL protocol messages

// writeUint32 writes a 32-bit unsigned integer in big-endian format
func writeUint32(w *bufio.Writer, val uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, val)
	_, err := w.Write(buf)
	return err
}

// readUint32 reads a 32-bit unsigned integer in big-endian format
func readUint32(r *bufio.Reader) (uint32, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}

// writeInt32 writes a 32-bit signed integer in big-endian format
func writeInt32(w *bufio.Writer, val int32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(val))
	_, err := w.Write(buf)
	return err
}

// readInt32 reads a 32-bit signed integer in big-endian format
func readInt32(r *bufio.Reader) (int32, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(buf)), nil
}

// ParseErrorMessage parses a PostgreSQL error message
func ParseErrorMessage(body []byte) string {
	// PostgreSQL error messages are formatted as:
	// field_type(1 byte) + message + null_terminator
	// Common field types: S=Severity, C=Code, M=Message

	if len(body) == 0 {
		return "unknown error"
	}

	var message strings.Builder
	i := 0

	for i < len(body) {
		if body[i] == 0 {
			break
		}

		fieldType := body[i]
		i++

		// Find the end of this field (null terminated)
		start := i
		for i < len(body) && body[i] != 0 {
			i++
		}

		if i > start {
			fieldValue := string(body[start:i])

			switch fieldType {
			case 'S': // Severity
				message.WriteString("Severity: " + fieldValue + "; ")
			case 'C': // Code
				message.WriteString("Code: " + fieldValue + "; ")
			case 'M': // Message
				message.WriteString("Message: " + fieldValue + "; ")
			case 'D': // Detail
				message.WriteString("Detail: " + fieldValue + "; ")
			case 'H': // Hint
				message.WriteString("Hint: " + fieldValue + "; ")
			}
		}

		if i < len(body) {
			i++ // Skip null terminator
		}
	}

	result := message.String()
	if result == "" {
		return "unknown error"
	}

	return strings.TrimSuffix(result, "; ")
}

// parseStartupParams parses startup message parameters
func parseStartupParams(data []byte) map[string]string {
	params := make(map[string]string)
	if len(data) == 0 {
		return params
	}

	// Convert to string and split by null bytes
	str := string(data)
	parts := strings.Split(str, "\x00")

	// Remove empty parts and final empty string if present
	var cleanParts []string
	for _, part := range parts {
		if part != "" {
			cleanParts = append(cleanParts, part)
		}
	}

	// Parse key-value pairs
	for i := 0; i < len(cleanParts)-1; i += 2 {
		if i+1 < len(cleanParts) {
			key := cleanParts[i]
			value := cleanParts[i+1]
			params[key] = value
		}
	}

	return params
}

// buildStartupMessage builds a startup message for backend connection
func buildStartupMessage(user, database string) []byte {
	params := fmt.Sprintf("user\x00%s\x00database\x00%s\x00\x00", user, database)
	msg := make([]byte, 8+len(params))
	binary.BigEndian.PutUint32(msg[0:4], uint32(len(msg)))
	binary.BigEndian.PutUint32(msg[4:8], 196608) // Protocol version 3.0
	copy(msg[8:], params)
	return msg
}

// buildMD5Response builds MD5 authentication response
func buildMD5Response(user, pass string, salt []byte) string {
	// MD5(password + user)
	h1 := md5.Sum([]byte(pass + user))
	hexStr := fmt.Sprintf("%x", h1)

	// MD5(hex + salt)
	h2 := md5.Sum(append([]byte(hexStr), salt...))
	return "md5" + fmt.Sprintf("%x", h2)
}

// parseNotificationResponse parses a notification response from the backend
func parseNotificationResponse(body []byte) *NotificationMessage {
	if len(body) < 8 {
		return nil
	}

	processID := int32(binary.BigEndian.Uint32(body[0:4]))

	// Parse channel and payload (null-terminated strings)
	rest := body[4:]
	nullPos := -1
	for i, b := range rest {
		if b == 0 {
			nullPos = i
			break
		}
	}
	if nullPos == -1 {
		return nil
	}

	channel := string(rest[:nullPos])
	payload := ""
	if nullPos+1 < len(rest) {
		payloadBytes := rest[nullPos+1:]
		if len(payloadBytes) > 0 && payloadBytes[len(payloadBytes)-1] == 0 {
			payload = string(payloadBytes[:len(payloadBytes)-1])
		} else {
			payload = string(payloadBytes)
		}
	}

	return &NotificationMessage{
		ProcessID: processID,
		Channel:   channel,
		Payload:   payload,
	}
}

// forwardMessage forwards a message from one connection to another
func forwardMessage(from *bufio.Reader, to *Client, msgType byte) error {
	// Read message length
	length, err := readUint32(from)
	if err != nil {
		return err
	}

	// Read message body (length includes the length field itself)
	bodyLength := int(length - 4)
	body := make([]byte, bodyLength)
	if bodyLength > 0 {
		if _, err := io.ReadFull(from, body); err != nil {
			return err
		}
	}

	return to.WriteMessage(msgType, body)
}

// forwardBackendMessage forwards a message from backend to client
func forwardBackendMessage(backend *Backend, client *Client, msgType byte) error {
	// Read message length
	length, err := readUint32(backend.reader)
	if err != nil {
		return err
	}

	// Read message body (length includes the length field itself)
	bodyLength := int(length - 4)
	body := make([]byte, bodyLength)
	if bodyLength > 0 {
		if _, err := io.ReadFull(backend.reader, body); err != nil {
			return err
		}
	}

	return client.WriteMessage(msgType, body)
}
