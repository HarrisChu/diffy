package pkg

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

type executor struct {
	cfg *Config
}

func NewExecutor(cfg *Config) *executor {
	return &executor{
		cfg: cfg,
	}
}

func (e *executor) Run() error {
	for _, job := range e.cfg.Jobs {
		log.Default().Println("Running job:", job.Name)
		if err := e.runJob(job); err != nil {
			log.Default().Printf("Job %s failed: %s\n", job.Name, err)
			return err
		} else {
			log.Default().Printf("Job %s succeeded\n", job.Name)
		}
	}
	return nil
}

func (e *executor) runJob(job *Job) error {
	if len(e.cfg.Statements) > 0 {
		for _, stmt := range e.cfg.Statements {
			cmd := exec.Command("sh", "-c", job.Shell)
			cmd.Env = os.Environ()
			statement := fmt.Sprintf("NEBULA_STATEMENT=%s", stmt)
			cmd.Env = append(cmd.Env, statement)
			log.Default().Printf("Executing statement: %s\n", stmt)
			for k, v := range e.cfg.Environments {
				if k == "NEBULA_STATEMENT" {
					continue
				}
				cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%v", k, v))
			}
			output, err := cmd.CombinedOutput()
			if err != nil {
				return fmt.Errorf("command execution failed: %s, output: %s", err, string(output))
			}
			log.Default().Printf("Command output: %s\n", string(output))
		}
	} else {
		cmd := exec.Command("sh", "-c", job.Shell)
		cmd.Env = os.Environ()
		for k, v := range e.cfg.Environments {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%v", k, v))
		}
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("command execution failed: %s, output: %s", err, string(output))
		}
		log.Default().Printf("Command output: %s\n", string(output))
	}
	return nil
}
