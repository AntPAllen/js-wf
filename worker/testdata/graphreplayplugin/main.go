package main

import (
	"js-wf/internal/graphreplayfixture"
	"js-wf/worker"
)

var Workflow = worker.WorkflowDefinition{
	Handler: graphreplayfixture.Initial,
	Continuations: map[string]worker.ContinuationHandler{
		"middle_v1": graphreplayfixture.Middle,
		"finish_v1": graphreplayfixture.Finish,
	},
}

var MissingContinuation = worker.WorkflowDefinition{
	Handler:       graphreplayfixture.Initial,
	Continuations: map[string]worker.ContinuationHandler{"middle_v1": graphreplayfixture.Middle},
}
