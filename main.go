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

func main() {
  repodir, err := findRepoRoot()
  if err != nil {
    log.Fatalln(err)
  }

  gitdir := filepath.Join(repodir, ".git")
  tankerdir := filepath.Join(gitdir, "tanker")
  loggingPath := filepath.Join(tankerdir, "logs")
  dataDir := filepath.Join(tankerdir, "tmp")
  confPath := filepath.Join(tankerdir, "config.yml")

  // Initialize logging to a file.
  err = storage.EnsurePath(loggingPath)
	if err != nil {
		log.Fatalln(err)
	}
	logfh, err := os.OpenFile(loggingPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	defer logfh.Close()
	log.SetOutput(logfh)

  // Initialize a directory for writing tanker data during download.
	err = storage.EnsureDir(dataDir)
	if err != nil {
		log.Fatalln(err)
	}

  // Load a tanker config file.
	conf := DefaultConfig()
  err = ParseConfigFile(confPath, &conf)
  if err != nil {
    log.Fatalln(err)
  }

  rootCmd := &cobra.Command{
    Use: "tanker",
  }

  initCmd := &cobra.Command{
    Use: "init <base url>",
    Args: cobra.ExactArgs(1),
    RunE: func(_ *cobra.Command, args []string) error {

      cmd := exec.Command("git", "config", "lfs.url", "tanker")
      err := cmd.Run()
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

      url := args[0]
      conf.BaseURL = url
      err = WriteConfigFile(conf, confPath)
      if err != nil {
        return fmt.Errorf("writing config file: %s", err)
      }

      return nil
    },
  }

  transferCmd := &cobra.Command{
    Use: "transfer",
    RunE: func(cmd *cobra.Command, args []string) error {
      return transfer(conf, dataDir)
    },
  }

  logsCmd := &cobra.Command{
    Use: "logs",
    RunE: func(cmd *cobra.Command, args []string) error {
			t, err := tail.TailFile(loggingPath, tail.Config{Follow: true})
      if err != nil {
        return err
      }
			for line := range t.Lines {
			  fmt.Println(line.Text)
			}
      return nil
    },
  }

  rootCmd.AddCommand(initCmd)
  rootCmd.AddCommand(transferCmd)
  rootCmd.AddCommand(logsCmd)

  err = rootCmd.Execute()
  if err != nil {
    log.Fatalln(err)
  }
}

// findRepoRoot finds the root of the repo.
func findRepoRoot() (string, error) {
  cmd := exec.Command("git", "rev-parse", "--show-toplevel")
  out, err := cmd.Output()
  if err != nil {
    return "", fmt.Errorf("finding .git directory: %s", err)
  }
  if len(out) == 0 {
    return "", fmt.Errorf("finding .git directory: empty output")
  }
  path := string(out)
  path = strings.TrimSpace(path)
  return path, nil
}
