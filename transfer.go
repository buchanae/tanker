package main

import (
	"context"
	"fmt"
	"log"
  "time"
	"os"
	"path/filepath"
	"runtime/debug"

  "github.com/buchanae/tanker/storage"
  "github.com/machinebox/progress"
)

// transfer implements the actual git-lfs transfer agent,
// which handles communication with git-lfs via stdin/out,
// downloading/uploading, etc.
func transfer(conf Config, dataDir string) error {

  if conf.BaseURL == "" {
    return fmt.Errorf("config BaseURL is required")
  }

  // Get a storage (swift, s3, etc) client.
  store, err := storage.NewStorage(conf.BaseURL, conf.Storage)
	if err != nil {
    return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

  // Start processing git-lfs messages
	comms := DefaultComms()
	for {
		msg, err := comms.Input()
		if err != nil {
      return err
		}

		err = handle(ctx, msg, comms, store, conf.BaseURL, dataDir)
		if err != nil {
      return err
		}

		if _, ok := msg.(*TerminateMessage); ok {
			break
		}
	}
  return nil
}

// handle handles a single input message from git-lfs (init, upload, download, etc)
func handle(
  ctx context.Context,
  m Message,
  comms *Comms,
  store storage.Storage,
  baseURL, dataDir string,
  ) (err error) {

  defer handlePanic(func(e error) {
    err = e
  })

	switch msg := m.(type) {
	case *InitMessage:
		comms.Initialized()
		return nil

	case *UploadMessage:
		url, err := store.Join(baseURL, msg.Oid)
		if err != nil {
			comms.SendError(msg.Oid, err)
			// A failed upload should not fail the whole process,
			// so we return nil. The error has been communicated
			// to git-lfs above.
			return nil
		}

    log.Println("Uploading", msg.Path, url)

    src, err := os.Open(msg.Path)
    if err != nil {
      return fmt.Errorf("opening source file %q: %s", err)
    }
    defer src.Close()

    // Set up progress monitoring.
    reader := progress.NewReader(src)
    watchCtx, cancel := context.WithCancel(ctx)
    defer cancel()
    go watchProgress(watchCtx, comms, msg.Oid, msg.Size, reader)

    // Start uploading
		_, err = store.Put(ctx, url, reader)
    cancel()

		if err != nil {
			comms.SendError(msg.Oid, err)
			// A failed upload should not fail the whole process,
			// so we return nil. The error has been communicated
			// to git-lfs above.
			return nil
		}

		return comms.SendComplete(msg.Oid, "")

	case *DownloadMessage:

		// determine path to download file to.
		// this usually goes into ".tanker/data".
		// git-lfs will handle moving the file from here.
		path := filepath.Join(dataDir, msg.Oid)
		abspath, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("determining download path: %s", err)
		}

		url, err := store.Join(baseURL, msg.Oid)
		if err != nil {
			comms.SendError(msg.Oid, err)
			// A failed download should not fail the whole process,
			// so we return nil. The error has been communicated
			// to git-lfs above.
			return nil
		}

    log.Println("Downloading", url, abspath)

    dest, err := os.Create(abspath)
    if err != nil {
      return fmt.Errorf("opening dest path %q: %s", abspath, dest)
    }

    // Set up progress monitoring
    writer := progress.NewWriter(dest)
    watchCtx, cancel := context.WithCancel(ctx)
    defer cancel()
    go watchProgress(watchCtx, comms, msg.Oid, msg.Size, writer)

    // Start downloading
		_, err = store.Get(ctx, url, writer)
    cancel()
    closeErr := dest.Close()

		if err != nil {
			// TODO probably need to ensure files are cleanup up on failed downloads.
			comms.SendError(msg.Oid, err)

			// A failed download should not fail the whole process,
			// so we return nil. The error has been communicated
			// to git-lfs above.
			return nil
		}

		if closeErr != nil {
			// TODO probably need to ensure files are cleanup up on failed downloads.
			comms.SendError(msg.Oid, closeErr)

			// A failed download should not fail the whole process,
			// so we return nil. The error has been communicated
			// to git-lfs above.
			return nil
		}

		return comms.SendComplete(msg.Oid, abspath)

	case *TerminateMessage:
		return nil
	default:
		return fmt.Errorf("unknown message type %#v", msg)
	}
}

// recover from panic and call "cb" with an error value.
func handlePanic(cb func(error)) {
	if r := recover(); r != nil {
		if e, ok := r.(error); ok {
			b := debug.Stack()
			cb(fmt.Errorf("panic: %s\n%s", e, string(b)))
		} else {
			cb(fmt.Errorf("Unknown worker panic: %+v", r))
		}
	}
}

// watchProgress watches the progress of a download/upload
// and emits git-lfs progess messages.
func watchProgress(ctx context.Context, comms *Comms, oid string, size int, c progress.Counter) {

  var last int
  t := progress.NewTicker(ctx, c, int64(size), time.Millisecond * 250)
  for p := range t {

    total := int(p.N())
    inc := total - last
    last = total

    comms.Send(&ProgressMessage{
      Event: "progress",
      Oid: oid,
      BytesSoFar: total,
      BytesSinceLast: inc,
    })
  }
}
