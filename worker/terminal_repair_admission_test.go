package worker_test

import (
	"js-wf/journal"
	"strings"
	"testing"
)

func TestWorkerTerminalOutcomeBeforeRepair(t *testing.T) {
	cases := terminalAdmissionCases()
	for name, raw := range map[string]string{
		"foreign_generation":          `{"inv_seq":2,"result":"NDI="}`,
		"zero_generation":             `{"inv_seq":0,"result":"NDI="}`,
		"absent_generation":           `{"result":"NDI="}`,
		"null_generation":             `{"inv_seq":null,"result":"NDI="}`,
		"completed_error":             `{"inv_seq":1,"error":"bad"}`,
		"failed_empty_error":          `{"inv_seq":1,"error":""}`,
		"failed_inline_result":        `{"inv_seq":1,"error":"bad","result":"NDI="}`,
		"valid_failed":                `{"inv_seq":1,"error":"stopped"}`,
		"valid_failed_opaque_request": `{"inv_seq":1,"error":"stopped","limit_request":{"user":1,"user":2}}`,
		"valid_empty_result":          `{"inv_seq":1}`,
	} {
		cases[name] = raw
	}
	for _, backend := range []string{"legacy", "canonical"} {
		for name, raw := range cases {
			t.Run(backend+"/"+name, func(t *testing.T) {
				valid := validTerminalAdmissionCase(name) || strings.HasPrefix(name, "valid_")
				kind := journal.Completed
				if strings.HasPrefix(name, "failed_") || strings.HasPrefix(name, "valid_failed") {
					kind = journal.Failed
				}
				runWorkerStepAdmission(t, backend, "terminal", "valid", func(e []journal.Entry) []journal.Entry {
					return []journal.Entry{e[0], {Index: 1, Kind: kind, Payload: []byte(raw)}}
				}, valid)
			})
		}
	}
}
