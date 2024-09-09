package cmd

import (
	"github.com/gkwa/manydonkey/core"
	"github.com/spf13/cobra"
)

var (
	copyFrom string
	copyTo   string
)

var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy data between SQLite databases",
	Run: func(cmd *cobra.Command, args []string) {
		logger := LoggerFrom(cmd.Context())
		copier := core.NewSQLiteCopier(logger)
		err := copier.CopyData(copyFrom, copyTo)
		if err != nil {
			logger.Error(err, "Failed to copy data")
		}
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)
	copyCmd.Flags().StringVar(&copyFrom, "copy-from", "", "Absolute path of the source SQLite database")
	copyCmd.Flags().StringVar(&copyTo, "copy-to", "", "Absolute path of the destination SQLite database")
	if err := copyCmd.MarkFlagRequired("copy-from"); err != nil {
		panic(err)
	}
	if err := copyCmd.MarkFlagRequired("copy-to"); err != nil {
		panic(err)
	}
}
