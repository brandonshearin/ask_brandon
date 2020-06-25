package pipeline

import "context"

// Payload is implemented by values that can be sent through a pipeline.
type Payload interface {
	// Clone returns a new Payload that is a deep-copy of the original.
	Clone() Payload

	// MarkAsProcessed is invoked by the pipeline when the Payload either
	// reaches the pipeline sink or it gets discarded by one of the
	// pipeline stages.
	MarkAsProcessed()
}

//Processor is implemented by types that can process Payloads as part of a pipeline stage
type Processor interface {
	/*
		Process operates on the input payload and returns back a new payload
		to be forwarded to the next pipeline stage.  Processors may also prevent
		the payload from reaching later stages by returning a nil payload instead
	*/
	Process(context.Context, Payload) (Payload, error)
}

//ProcessorFunc is an adapter to allow the use of plain functions
//as Processor instances.  If f is a function with the appropriate signature,
//ProcessorFunc(f) is a Processor that calls f.
type ProcessorFunc func(ctx context.Context, p Payload) (Payload, error)

//Process calls f(ctx, p)
func (f ProcessorFunc) Process(ctx context.Context, p Payload) (Payload, error) {
	return f(ctx, p)
}

//StageRunner is implemented by types that can be strung together to form a multi-stage pipeline
type StageRunner interface {
	/*Run implements the processing logic for a stage by reading
	incoming Payloads from an input channel, processing them and
	outputting the results to an output channel

	Calls to Run are expected to block until:
	- the stage input channel is closed OR
	- the provided context expires OR
	- an error occurs while processing payloads */
	Run(context.Context, StageParams)
}

/*StageParams encapsulates the info required for executing a pipeline stage.
The pipeline passes a StageParams instance to the Run() method of each stage.*/
type StageParams interface {
	//StageIndex returns the position of this stage in the pipeline for annotation purposes
	StageIndex() int

	//Input returns a channel for reading the input payloads for a stage
	Input() <-chan Payload

	//Output returns a channel for writing the output payloads for a stage
	Output() chan<- Payload

	//Error returns a channel for writing errors encountered by a stage during processing
	Error() chan<- error
}

/*Source is implemnted by types that generate Payload instances which can be used
as inputs to a Pipeline*/
type Source interface {
	Next(context.Context) bool
	Payload() Payload
	Error() error
}

//Sink is implemented by types that can operate as the tail of a pipeline
type Sink interface {
	//Consume processes a Payload instance that has been emitted out of a Pipeline instance
	Consume(context.Context, Payload) error
}
