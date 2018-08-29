package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// All this is based on git-lfs custom transfer agents.
// In particular, this is a "standalone transfer agent"
// https://github.com/git-lfs/git-lfs/blob/master/docs/custom-transfers.md

func main() {
	conf := DefaultConfig()

	// TODO concurrent, multiprocess log
	logfh, err := os.Create(conf.Logging.Path)
	if err != nil {
		log.Fatal(err)
	}
	defer logfh.Close()
	log.SetOutput(logfh)
	defer log.Println("tanker done")

	err = EnsureDir(conf.DataDir)
	if err != nil {
		log.Fatal(err)
	}

	store, err := NewSwiftRetrier(SwiftConfig{})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	comms := DefaultComms()
	for {
		msg, err := comms.Input()
		if err != nil {
			log.Fatal(err)
		}

		err = handle(ctx, msg, comms, store, conf)
		if err != nil {
			log.Fatal(err)
		}

		if _, ok := msg.(*TerminateMessage); ok {
			break
		}
	}
}

// handle handles a single input message from git-lfs (init, upload, download, etc)
func handle(ctx context.Context, m Message, comms *Comms, store Storage, conf Config) error {
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

		_, err = store.Put(ctx, url, msg.Path)
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

		_, err = store.Get(ctx, url, abspath)
		if err != nil {
			// TODO probably need to ensure files are cleanup up on failed downloads.
			comms.SendError(msg.Oid, err)

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
