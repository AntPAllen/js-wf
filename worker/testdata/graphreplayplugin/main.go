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

var ChildWorkflow = worker.WorkflowDefinition{
	Handler: graphreplayfixture.ChildInitial,
	Continuations: map[string]worker.ContinuationHandler{
		"child_middle_v1": graphreplayfixture.ChildMiddle,
		"child_finish_v1": graphreplayfixture.ChildFinish,
	},
}

var MissingChildContinuation = worker.WorkflowDefinition{
	Handler:       graphreplayfixture.ChildInitial,
	Continuations: map[string]worker.ContinuationHandler{"child_middle_v1": graphreplayfixture.ChildMiddle},
}
