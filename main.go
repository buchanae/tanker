package main

import (
  "bufio"
  "log"
  "os"
  "encoding/json"
)

type message struct {
  Event string `json:"event"`
}

type initMessage struct {
  Operation string `json:"operation"`
  Remote string `json:"remote"`
  Concurrent bool `json:"concurrent"`
  ConcurrentTransfers int `json:"concurrenttransfers"`
}

type uploadMessage struct {
  Oid string `json:"oid"`
  Size int `json:"size"`
  Path string `json:"path"`
}

type downloadMessage struct {
  Oid string `json:"oid"`
  Size int `json:"size"`
}

type progressMessage struct {
  Event string `json:"event"`
  Oid string `json:"oid"`
  BytesSoFar int `json:"bytesSoFar"`
  BytesSinceLast int `json:"bytesSinceLast"`
}

type completeMessage struct {
  Event string `json:"event"`
  Oid string `json:"oid"`
  Path string `json:"path"`
}

type errorMessage struct {
  Event string `json:"event"`
  Oid string `json:"oid"`
  Error struct {
    Code int `json:"code"`
    Message string `json:"message"`
  } `json:"error"`
}

func main() {
  // TODO concurrent, multiprocess log
  logfh, err := os.Create("tanker.log")
  if err != nil {
    log.Fatal(err)
  }
  defer logfh.Close()
  log.SetOutput(logfh)

  scanner := bufio.NewScanner(os.Stdin)
  enc := json.NewEncoder(os.Stdout)

  defer log.Println("tanker done")

  for scanner.Scan() {

    var msg message
    err := json.Unmarshal(scanner.Bytes(), &msg)
    if err != nil {
      log.Fatal(err)
    }

    switch msg.Event {
    case "init":
      var msg initMessage
      err := json.Unmarshal(scanner.Bytes(), &msg)
      if err != nil {
        log.Fatal(err)
      }

      log.Println(msg)

      var empty struct{}
      enc.Encode(empty)

    case "upload":
      var msg uploadMessage
      err := json.Unmarshal(scanner.Bytes(), &msg)
      if err != nil {
        log.Fatal(err)
      }

      log.Println(msg)

      err = enc.Encode(progressMessage{
        Event: "progress",
        Oid: msg.Oid,
        BytesSoFar: msg.Size,
        BytesSinceLast: msg.Size,
      })
      if err != nil {
        log.Fatal(err)
      }

      err = enc.Encode(completeMessage{
        Event: "complete",
        Oid: msg.Oid,
      })
      if err != nil {
        log.Fatal(err)
      }

    case "download":
      var msg downloadMessage
      err := json.Unmarshal(scanner.Bytes(), &msg)
      if err != nil {
        log.Fatal(err)
      }

      log.Println(msg)

      err = enc.Encode(completeMessage{
        Event: "complete",
        Oid: msg.Oid,
        Path: "foo-dne",
      })
      if err != nil {
        log.Fatal(err)
      }

    case "terminate":
      return

    default:
      log.Println("unknown event", msg.Event)
    }
  }

  err = scanner.Err()
  if err != nil {
    log.Fatal(err)
  }
}
