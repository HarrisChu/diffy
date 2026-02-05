package pkg

import (
	"fmt"

	"github.com/spf13/cobra"
)

var configPath string

var rootCmd = &cobra.Command{
	Use: "diffy",
	RunE: func(cmd *cobra.Command, args []string) error {
		if configPath == "" {
			return fmt.Errorf("config path is required")
		}
		config, err := NewConfigFromFile(configPath)
		if err != nil {
			return err
		}
		exec := NewExecutor(config)
		if err := exec.Run(); err != nil {
			return err
		}
		return nil
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.Flags().StringVarP(&configPath, "config", "c", "", "path to config file")

}
