package main

import (
	"fmt"
	"io"
	"os"

	"js-wf/workflowlint"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(paths []string, out io.Writer) error {
	findings, err := workflowlint.Scan(paths...)
	if err != nil {
		return err
	}
	for _, finding := range findings {
		if _, err := fmt.Fprintf(out, "%s:%d:%d: %s inside workflow handler; %s\n", finding.File, finding.Line, finding.Column, finding.Call, finding.Hint); err != nil {
			return err
		}
	}
	if len(findings) > 0 {
		return fmt.Errorf("%d nondeterministic workflow calls found", len(findings))
	}
	return nil
}
