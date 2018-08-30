package main

import (
	"bufio"
  "log"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// comms manages communication with git-lfs
// https://github.com/git-lfs/git-lfs/blob/master/docs/custom-transfers.md
type Comms struct {
	enc     *json.Encoder
	scanner *bufio.Scanner
}

func DefaultComms() *Comms {
	return NewComms(os.Stdin, os.Stdout)
}

func NewComms(in io.Reader, out io.Writer) *Comms {

	// Read git-lfs messages from in (usually stdin)
	scanner := bufio.NewScanner(in)
	// Write git-lfs messages to out (usually stdout)
	enc := json.NewEncoder(out)

	return &Comms{
		enc:     enc,
		scanner: scanner,
	}
}

func (c *Comms) Input() (Message, error) {
	more := c.scanner.Scan()
	err := c.scanner.Err()
	if err != nil {
		return nil, fmt.Errorf("scanning for input message: %s", err)
	}
  if err == io.EOF || !more {
		return &TerminateMessage{}, nil
  }

	// Determine the type of the message by looking for the "event" field.
	var msg genericMessage
	err = json.Unmarshal(c.scanner.Bytes(), &msg)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling message wrapper: %s", err)
	}

	switch msg.Event {
	case "init":
		msg := &InitMessage{}
		err := json.Unmarshal(c.scanner.Bytes(), msg)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling init message: %s", err)
		}
		return msg, nil
	case "upload":
		msg := &UploadMessage{}
		err := json.Unmarshal(c.scanner.Bytes(), msg)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling upload message: %s", err)
		}
		return msg, nil
	case "download":
		msg := &DownloadMessage{}
		err := json.Unmarshal(c.scanner.Bytes(), msg)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling download message: %s", err)
		}
		return msg, nil
	case "terminate":
		return &TerminateMessage{}, nil
	default:
		return nil, fmt.Errorf("unknown message type: %q", msg.Event)
	}
}

// Initialized signals to git-lfs that tanker has successfully initialized.
func (c *Comms) Initialized() {
	var empty struct{}
	c.enc.Encode(empty)
}

func (c *Comms) Send(msg Message) error {
	err := c.enc.Encode(msg)
	if err != nil {
		return fmt.Errorf("sending message: %s")
	}
	return nil
}

func (c *Comms) SendError(oid string, err error) {
  log.Println("Sending error", oid, err)
	// We're ignoring the error from Send();
	// if the send fails, there's not a lot we can do.
	c.Send(&ErrorMessage{
		Event: "error",
		Oid:   oid,
		Error: ErrorDetail{
			// TODO is there a better code?
			Code:    1,
			Message: err.Error(),
		},
	})
}

func (c *Comms) SendComplete(oid, path string) error {
	return c.Send(&CompleteMessage{
		Event: "complete",
		Oid:   oid,
    Path: path,
	})
}

type Message interface {
	isMessage()
}

// genericMessage is used to get the "event" field,
// in order to determine what type of message to parse.
type genericMessage struct {
	Event string `json:"event"`
}

type InitMessage struct {
	Operation           string `json:"operation"`
	Remote              string `json:"remote"`
	Concurrent          bool   `json:"concurrent"`
	ConcurrentTransfers int    `json:"concurrenttransfers"`
}

type UploadMessage struct {
	Oid  string `json:"oid"`
	Size int    `json:"size"`
	Path string `json:"path"`
}

type DownloadMessage struct {
	Oid  string `json:"oid"`
	Size int    `json:"size"`
}

type ProgressMessage struct {
	Event          string `json:"event"`
	Oid            string `json:"oid"`
	BytesSoFar     int    `json:"bytesSoFar"`
	BytesSinceLast int    `json:"bytesSinceLast"`
}

type CompleteMessage struct {
	Event string `json:"event"`
	Oid   string `json:"oid"`
	Path  string `json:"path"`
}

type ErrorMessage struct {
	Event string      `json:"event"`
	Oid   string      `json:"oid"`
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type TerminateMessage struct{}

func (m *InitMessage) isMessage()      {}
func (m *UploadMessage) isMessage()    {}
func (m *DownloadMessage) isMessage()  {}
func (m *ProgressMessage) isMessage()  {}
func (m *CompleteMessage) isMessage()  {}
func (m *ErrorMessage) isMessage()     {}
func (m *TerminateMessage) isMessage() {}
