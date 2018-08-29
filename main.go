package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"

  "github.com/buchanae/tanker/storage"
)

// All this is based on git-lfs custom transfer agents.
// In particular, this is a "standalone transfer agent"
// https://github.com/git-lfs/git-lfs/blob/master/docs/custom-transfers.md

func main() {
	conf := DefaultConfig()

  err := storage.EnsurePath(conf.Logging.Path)
	if err != nil {
		log.Fatal(err)
	}

	// TODO concurrent, multiprocess log
	logfh, err := os.Create(conf.Logging.Path)
	if err != nil {
		log.Fatal(err)
	}
	defer logfh.Close()
	log.SetOutput(logfh)
	defer log.Println("tanker done")

  // TODO probably want the git repo root, not the current directory
	err = storage.EnsureDir(conf.DataDir)
	if err != nil {
		log.Fatal(err)
	}

  store, err := storage.NewStorage(conf.BaseURL, conf.Storage)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	comms := DefaultComms()
	for {
		msg, err := comms.Input()
		if err != nil {
      log.Println("input err")
			log.Fatal(err)
		}

		err = handle(ctx, msg, comms, store, conf)
		if err != nil {
      log.Println("handle err")
			log.Fatal(err)
		}

		if _, ok := msg.(*TerminateMessage); ok {
			break
		}
	}
}

// handle handles a single input message from git-lfs (init, upload, download, etc)
func handle(ctx context.Context, m Message, comms *Comms, store storage.Storage, conf Config) (err error) {

  defer handlePanic(func(e error) {
    err = e
  })

	switch msg := m.(type) {
	case *InitMessage:
		comms.Initialized()
		return nil

	case *UploadMessage:
		url, err := store.Join(conf.BaseURL, msg.Oid)
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

    log.Println("Put")
		_, err = store.Put(ctx, url, src)
		if err != nil {
      log.Println("Upload failed", err)
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
		path := filepath.Join(conf.DataDir, msg.Oid)
		abspath, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("determining download path: %s", err)
		}

		url, err := store.Join(conf.BaseURL, msg.Oid)
		if err != nil {
			comms.SendError(msg.Oid, err)
			// A failed download should not fail the whole process,
			// so we return nil. The error has been communicated
			// to git-lfs above.
			return nil
		}

    dest, err := os.Create(abspath)
    if err != nil {
      return fmt.Errorf("opening dest path %q: %s", abspath, dest)
    }

		_, err = store.Get(ctx, url, dest)
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
