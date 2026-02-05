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
		if output, err := e.runJob(job); err != nil {
			log.Default().Printf("Job %s failed: %s\n", job.Name, string(output))
			return err
		} else {
			log.Default().Printf("Job %s succeeded: %s\n", job.Name, string(output))
		}
	}
	return nil
}

func (e *executor) runJob(job *Job) ([]byte, error) {
	cmd := exec.Command("sh", "-c", job.Shell)
	cmd.Env = os.Environ()
	for k, v := range e.cfg.Environments {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%v", k, v))
	}
	return cmd.CombinedOutput()
}
