package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
  "strings"
	"syscall"

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
		return nil, err
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
    SilenceUsage: true,
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

      tanker, err := NewTanker()
      if err != nil {
        return err
      }
      defer tanker.Close()

      cmd := exec.Command("git", "lfs", "install", "--local")
      err = cmd.Run()
      if err != nil {
        return fmt.Errorf("configuring git-lfs: %s", err)
      }


      cmd = exec.Command("git", "config", "lfs.standalonetransferagent", "tanker")
      err = cmd.Run()
      if err != nil {
        return fmt.Errorf("configuring git-lfs: %s", err)
      }

      cmd = exec.Command("git", "config", "lfs.customtransfer.tanker.path", "tanker")
      err = cmd.Run()
      if err != nil {
        return fmt.Errorf("configuring git-lfs: %s", err)
      }

      cmd = exec.Command("git", "config", "lfs.customtransfer.tanker.args", "transfer")
      err = cmd.Run()
      if err != nil {
        return fmt.Errorf("configuring git-lfs: %s", err)
      }

			cmd = exec.Command("git", "config", "lfs.url", url)
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

  includeCmd := &cobra.Command{
		Use: "include",
		RunE: func(_ *cobra.Command, args []string) error {
      tanker, err := NewTanker()
      if err != nil {
        return err
      }
      defer tanker.Close()

      if len(args) == 0 {
        return fmt.Errorf("missing file list")
      }

      cmd := exec.Command("git", "config", "--get", "lfs.fetchinclude")
      out, err := cmd.Output()
			code := getExitCode(err)
			// exit code 1 means the config doesn't exist, which is ok in this case.
			// so we need special handling for that code here.
      if code == 1 {
        err = nil
      }
      if err != nil {
				return fmt.Errorf("getting lfs.fetchinclude config: %s", err)
			}
      sout := string(out)
      sout = strings.TrimSpace(sout)

      uniq := map[string]bool{}

      existingList := strings.Split(sout, ",")
      for _, key := range existingList {
        if key != "" {
          uniq[key] = true
        }
      }

      for _, key := range args {
        if key != "" {
          uniq[key] = true
        }
      }

      var keys []string
      for key, _ := range uniq {
        keys = append(keys, key)
      }

      list := strings.Join(keys, ",")

      cmd = exec.Command("git", "config", "lfs.fetchinclude", list)
      err = cmd.Run()
      if err != nil {
        return fmt.Errorf("setting lfs.fetchinclude config: %s", err)
      }

      cmd = exec.Command("git", "lfs", "pull", "--include", strings.Join(args, ","))
      cmd.Stdout = os.Stdout
      cmd.Stderr = os.Stderr
      err = cmd.Run()
      if err != nil {
        return err
      }

      return nil
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
  rootCmd.AddCommand(includeCmd)
  rootCmd.AddCommand(versionCmd)
  if err := rootCmd.Execute(); err != nil {
    os.Exit(1)
  }
}

// findRepoRoot finds the root of the repo.
func findRepoRoot() (string, error) {
  cmd := exec.Command("git", "rev-parse", "--show-toplevel")
  out, err := cmd.CombinedOutput()
  if err != nil {
    if strings.HasPrefix(string(out), "fatal: not a git repository") {
			return "", fmt.Errorf("not in a git repository")
		}
    return "", fmt.Errorf("%s", out)
  }
  if len(out) == 0 {
    return "", fmt.Errorf("finding repo root: empty output")
  }
  path := string(out)
  path = strings.TrimSpace(path)
  return path, nil
}


func getExitCode(err error) int {
	if err == nil {
		return 0
	}
	if exiterr, ok := err.(*exec.ExitError); ok {
    // The program has exited with an exit code != 0

    // This works on both Unix and Windows. Although package
    // syscall is generally platform dependent, WaitStatus is
    // defined for both Unix and Windows and in both cases has
    // an ExitStatus() method with the same signature.
    if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
      return status.ExitStatus()
    }
	}
	return 128
}
