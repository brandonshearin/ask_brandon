package pipeline

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"
)

type workerParams struct {
	stage int

	//channels for the worker's input, output and errors
	inCh  <-chan Payload
	outCh chan<- Payload
	errCh chan<- error
}

//Make workerParams implmement StageParams interface
func (p *workerParams) StageIndex() int        { return p.stage }
func (p *workerParams) Input() <-chan Payload  { return p.inCh }
func (p *workerParams) Output() chan<- Payload { return p.outCh }
func (p *workerParams) Error() chan<- error    { return p.errCh }

type Pipeline struct {
	stages []StageRunner
}

//New returns a new pipeline instance where input payloads will traverse each
//one of the specified stages
func New(stages ...StageRunner) *Pipeline {
	return &Pipeline{
		stages: stages,
	}
}

/*Process reads the contents of the provided source, sending them through the
stages of the pipeline and directs the results to the specified sink.  Returns
any errors that have occured.

Calls to Process block until:
- all data from the source has been processed OR
- an error occurs OR
- the supplied context expires
*/
func (p *Pipeline) Process(
	ctx context.Context,
	source Source,
	sink Sink) error {

	var wg sync.WaitGroup
	pCtx, ctxCancelFn := context.WithCancel(ctx)

	//Allocate channels for wiring together the source, stages, and sink
	stageCh := make([]chan Payload, len(p.stages)+1)
	errCh := make(chan error, len(p.stages)+2) //buffered channel provides enough space to hold potential error for each pipeline stage including source/sink
	for i := 0; i < len(stageCh); i++ {
		stageCh[i] = make(chan Payload)
	}

	//start a worker for each stage
	for i := 0; i < len(p.stages); i++ {
		wg.Add(1)
		go func(stageIndex int) {
			p.stages[stageIndex].Run(pCtx, &workerParams{
				stage: stageIndex,
				inCh:  stageCh[stageIndex],
				outCh: stageCh[stageIndex+1], //the output channel of nth worker is input channel of worker n+1
				errCh: errCh,
			})

			//once the Run() method of work n returns, its output channel is closed to
			//signal the next stage of the pipeline that no more data is available
			close(stageCh[stageIndex+1])
			wg.Done()
		}(i)

	}

	//spawn 2 additional workers, one for input source and one for output sink
	wg.Add(2)
	go func() {
		sourceWorker(pCtx, source, stageCh[0], errCh)
		close(stageCh[0])
		wg.Done()
	}()

	go func() {
		sinkWorker(pCtx, sink, stageCh[len(stageCh)-1], errCh)
		wg.Done()
	}()

	//spawn one final routine to act as a monitor.  it waits
	//for all workers to complete before closing shared error channel
	//and cancelling the wrapped context
	go func() {
		wg.Wait()
		close(errCh)
		ctxCancelFn()
	}()

	//collect any emitted errors and wrap them in a multi-error.
	//if any error gets published to the shared error channel,
	//the wrapped context will be cancelled to trigger a shutdown
	//of the entire pipeline.  ALSO- the preceeding for loop blocks
	//indefinitely if no errors reported, which is until
	//the monitor routine^^ closes errCh
	var err error
	for pErr := range errCh {
		err = multierror.Append(err, pErr)
		ctxCancelFn()
	}

	return err
}

/*to facilitate the asynchronous polling of the input source,
this function will be run inside a goroutine.  Its primary task is to iterate
the data source and publish each incoming payload to the specified channel: */
func sourceWorker(
	ctx context.Context,
	source Source,
	outCh chan<- Payload,
	errCh chan<- error) {

	for source.Next(ctx) {
		payload := source.Payload()
		select {
		case outCh <- payload:
		case <-ctx.Done():
			return //shutdown
		}
	}

	//before returning, check for any errors reported by the input source and
	//publish them to the provided error channel
	if err := source.Error(); err != nil {
		wrappedErr := xerrors.Errorf("pipeline source: %w", err)
		maybeEmitError(wrappedErr, errCh)
	}

}

//reads payloads from the provided input channel and attempts to publish them to the provided Sink instance.
func sinkWorker(
	ctx context.Context,
	sink Sink,
	inCh <-chan Payload,
	errCh chan<- error) {
	for {
		select {
		case payload, ok := <-inCh:
			if !ok {
				return
			}
			if err := sink.Consume(ctx, payload); err != nil {
				wrappedErr := xerrors.Errorf("pipeline sink: %w", err)
				maybeEmitError(wrappedErr, errCh)
				return
			}
			payload.MarkAsProcessed()
		case <-ctx.Done():
			return
		}
	}
}
