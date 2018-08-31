package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
  "strings"

  "github.com/spf13/cobra"
  "github.com/buchanae/tanker/storage"
	"github.com/hpcloud/tail"
)

// All this is based on git-lfs custom transfer agents.
// In particular, this is a "standalone transfer agent"
// https://github.com/git-lfs/git-lfs/blob/master/docs/custom-transfers.md

type Tanker struct {
  // Holds paths to commonly used files.
  Paths struct {
    Repo, Git, Tanker, Logs, Data, Config string
  }
  Config Config
  LogFile *os.File
}

func (t *Tanker) Close() error {
	if t.LogFile != nil {
		t.LogFile.Close()
	}
  return nil
}

func NewTanker() (*Tanker, error) {
  repodir, err := findRepoRoot()
  if err != nil {
		if strings.HasPrefix(err.Error(), "fatal: not a git repository") {
			// It's ok if we're not in a git repo, we'll just skip some steps.
			err = nil
		} else {
			return nil, fmt.Errorf("finding git repo root: %s", err)
		}
  }

  tanker := &Tanker{}

	if repodir != "" {
		tanker.Paths.Repo = repodir
		tanker.Paths.Git = filepath.Join(tanker.Paths.Repo, ".git")
		tanker.Paths.Tanker = filepath.Join(tanker.Paths.Git, "tanker")
		tanker.Paths.Logs = filepath.Join(tanker.Paths.Tanker, "logs")
		tanker.Paths.Data = filepath.Join(tanker.Paths.Tanker, "data")
		tanker.Paths.Config = filepath.Join(tanker.Paths.Tanker, "config.yml")

		// Initialize logging to a file.
		err = storage.EnsurePath(tanker.Paths.Logs)
		if err != nil {
			return nil, fmt.Errorf("initializing logging file: %s", err)
		}
		logfh, err := os.OpenFile(tanker.Paths.Logs, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("opening logging file: %s", err)
		}
		tanker.LogFile = logfh
		log.SetOutput(logfh)

		// Initialize a directory for writing tanker data during download.
		err = storage.EnsureDir(tanker.Paths.Data)
		if err != nil {
			return nil, fmt.Errorf("initializing data directory: %s", err)
		}

		tanker.Config = DefaultConfig()

		// Ensure the config file exists.
		if _, err := os.Open(tanker.Paths.Config); os.IsNotExist(err) {
			err := WriteConfigFile(tanker.Config, tanker.Paths.Config)
			if err != nil {
				return nil, fmt.Errorf("writing default config file: %s", err)
			}
		}

		// Load a tanker config file.
		err = ParseConfigFile(tanker.Paths.Config, &tanker.Config)
		if err != nil {
			return nil, fmt.Errorf("parsing config: %s", err)
		}
	}

  return tanker, nil
}

func main() {

  rootCmd := &cobra.Command{
    Use: "tanker",
  }

  initCmd := &cobra.Command{
    Use: "init <base url>",
    Args: cobra.ExactArgs(1),
    RunE: func(_ *cobra.Command, args []string) error {
			url := args[0]

			if len(url) == 0 {
				return fmt.Errorf("empty URL")
			}

			if url == "swift://" {
				return fmt.Errorf("invalid URL: a bucket name is required") 
			}

			if !strings.HasPrefix(url, "swift://") {
				return fmt.Errorf("invalid URL: tanker currently only supports swift://")
			}

			key := fmt.Sprintf("lfs.%s.standalonetransferagent", url)
      cmd := exec.Command("git", "config", "--global", key, "tanker")
      err := cmd.Run()
      if err != nil {
        return fmt.Errorf("configuring git-lfs: %s", err)
      }

      cmd = exec.Command("git", "config", "--global", "lfs.customtransfer.tanker.path", "tanker")
      err = cmd.Run()
      if err != nil {
        return fmt.Errorf("configuring git-lfs: %s", err)
      }

      cmd = exec.Command("git", "config", "--global", "lfs.customtransfer.tanker.args", "transfer")
      err = cmd.Run()
      if err != nil {
        return fmt.Errorf("configuring git-lfs: %s", err)
      }

      tanker, err := NewTanker()
      if err != nil {
        return err
      }
      defer tanker.Close()

			// If we're in a git repo, set up the repo.
			if tanker.Paths.Repo != "" {

				cmd := exec.Command("git", "config", "-f", ".lfsconfig", "lfs.url", url)
				err = cmd.Run()
				if err != nil {
					return fmt.Errorf("configuring git-lfs: %s", err)
				}

				// TODO just derive from lfs.url
				tanker.Config.BaseURL = url
				err = WriteConfigFile(tanker.Config, tanker.Paths.Config)
				if err != nil {
					return fmt.Errorf("writing config file: %s", err)
				}
			}

      return nil
    },
  }

  transferCmd := &cobra.Command{
    Use: "transfer",
    RunE: func(cmd *cobra.Command, args []string) error {

      tanker, err := NewTanker()
      if err != nil {
        return err
      }
      defer tanker.Close()

      return transfer(tanker.Config, tanker.Paths.Data)
    },
  }

  logsCmd := &cobra.Command{
    Use: "logs",
    RunE: func(cmd *cobra.Command, args []string) error {

      tanker, err := NewTanker()
      if err != nil {
        return err
      }
      defer tanker.Close()

			t, err := tail.TailFile(tanker.Paths.Logs, tail.Config{Follow: true})
      if err != nil {
        return err
      }
			for line := range t.Lines {
			  fmt.Println(line.Text)
			}
      return nil
    },
  }

  versionCmd := &cobra.Command{
    Use: "version",
    Run: func(cmd *cobra.Command, args []string) {
      fmt.Println(VersionString())
    },
  }

  rootCmd.AddCommand(initCmd)
  rootCmd.AddCommand(transferCmd)
  rootCmd.AddCommand(logsCmd)
  rootCmd.AddCommand(versionCmd)

  err := rootCmd.Execute()
  if err != nil {
    log.Fatalln(err)
  }
}

// findRepoRoot finds the root of the repo.
func findRepoRoot() (string, error) {
  cmd := exec.Command("git", "rev-parse", "--show-toplevel")
  out, err := cmd.CombinedOutput()
  if err != nil {
    return "", fmt.Errorf("%s", out)
  }
  if len(out) == 0 {
    return "", fmt.Errorf("empty output")
  }
  path := string(out)
  path = strings.TrimSpace(path)
  return path, nil
}
