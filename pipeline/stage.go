package pipeline

import (
	"context"
	"sync"

	"golang.org/x/xerrors"
)

type fifo struct {
	proc Processor
}

/*FIFO returns a StageRunner that processes incoming payloads in
a fifo fashion.  Each input is passed to the specified processor
and its output is emitted to the next stage*/
func FIFO(proc Processor) StageRunner {
	return fifo{
		proc: proc,
	}
}

func (r fifo) Run(ctx context.Context, params StageParams) {
	/*Run is designed to be blocking.  It runs this infinite for loop
	which does one of the following:
	- Monitors the provided context for cancellation, exiting from main loop if so
	- Attempts to retrieve the next payload from input channel, exiting main loop if chan closed*/
	for {
		select {
		case <-ctx.Done():
			return //cleanly shut down
		case payloadIn, ok := <-params.Input():
			if !ok {
				return //no more data. inpyt channel closed
			}

			//Once input payload received, process payload using user-defined processor
			payloadOut, err := r.proc.Process(ctx, payloadIn)
			if err != nil {
				wrapperErr := xerrors.Errorf("pipeline stage %d: %w", params.StageIndex(), err)
				maybeEmitError(wrapperErr, params.Error())
				return
			}

			//if the processor returned a nil payload it should be discarded.  Continue with the next iteration of for loop
			if payloadOut == nil {
				payloadIn.MarkAsProcessed()
				continue
			}

			select {
			case params.Output() <- payloadOut:
			case <-ctx.Done():
				return //cleanly shut down
			}
		}
	}
}

//maybeEmitError attempts to queue err to a buffered error channel.  If channel is full, the error is dropped
func maybeEmitError(err error, errCh chan<- error) {
	select {
	case errCh <- err: //error emitted
	default: //error channel is full with other errors
	}

}

type fixedWorkerPool struct {
	fifos []StageRunner
}

/*FixedWorkerPool returns a StageRunner that spins up a pool containing
numWorkers to process incoming payloads in parallel and emit their outputs
to the next stage*/
func FixedWorkerPool(proc Processor, numWorkers int) StageRunner {
	if numWorkers <= 0 {
		panic("FixedWorkerPool: numWorkers must be > 0")
	}

	fifos := make([]StageRunner, numWorkers)
	for i := 0; i < numWorkers; i++ {
		fifos[i] = FIFO(proc)
	}

	return &fixedWorkerPool{
		fifos: fifos,
	}
}

//Run implements stage runner
func (p *fixedWorkerPool) Run(ctx context.Context, params StageParams) {
	var wg sync.WaitGroup

	//Spin up each worker in the pool and wait for them to exit
	for i := 0; i < len(p.fifos); i++ {
		wg.Add(1)
		go func(fifoIndex int) {
			p.fifos[fifoIndex].Run(ctx, params)
		}(i)
		wg.Done()
	}

	wg.Wait()
}

type dynamicWorkerPool struct {
	proc      Processor
	tokenPool chan struct{}
}

/*DynamicWorkerPool returns a StageRunner that maintains a dynamic worker pool that can
scale up to maxWorkers for processing incoming inputs in parallel
and emitting their outputs to the next stage*/
func DynamicWorkerPool(proc Processor, maxWorkers int) StageRunner {
	if maxWorkers <= 0 {
		panic("DynamicWorkerPool: maxWorkers must be > 0")
	}

	//pre-populate token pool with max number of workers we wish to allow
	tokenPool := make(chan struct{}, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		tokenPool <- struct{}{}
	}

	return &dynamicWorkerPool{
		proc:      proc,
		tokenPool: tokenPool,
	}
}

func (p *dynamicWorkerPool) Run(ctx context.Context, params StageParams) {
stop:
	for {
		select {
		case <-ctx.Done():
			break stop //cleanly shut down
		case payloadIn, ok := <-params.Input():
			if !ok {
				break stop
			}
			//Process payload

			/*This select statement blocks until a token can be read off the token pool.
			This code serves as a choke point for limiting the number of concurrent workers.
			Once all tokens in the pool are exhausted, attempts to read off channel will be blocked until
			a token is returned to the pool, which happens below during the processing step*/
			var token struct{}
			select {
			case token = <-p.tokenPool:
			case <-ctx.Done():
				break stop
			}

			/*More or less the same as the FIFO implementation, with 2 small differences
			- executes inside a go routine
			- includes a defer statement to ensure that token is returned to the pool.  Ensures token
			is available for reuse*/
			go func(payloadIn Payload, token struct{}) {
				defer func() { p.tokenPool <- token }()
				payloadOut, err := p.proc.Process(ctx, payloadIn)
				if err != nil {
					wrappedErr := xerrors.Errorf("pipeline stage %d: %w", params.StageIndex(), err)
					maybeEmitError(wrappedErr, params.Error())
					return
				}

				if payloadOut == nil {
					payloadIn.MarkAsProcessed()
					return // Discard payload
				}

				select {
				case params.Output() <- payloadOut:
				case <-ctx.Done():
				}
			}(payloadIn, token)
		}
	}

	//Reclaim all tokens to ensure that the dynamic pool does not leak any goroutines.
	//We can achieve same effect with sync.waitgroup.
	for i := 0; i < cap(p.tokenPool); i++ {
		<-p.tokenPool
	}
}

type broadcast struct {
	fifos []StageRunner
}

//Broadcast receives a list of processor instances and creates a FIFO instance for each one.
func Broadcast(procs ...Processor) StageRunner {
	if len(procs) == 0 {
		panic("Broadcast: at least one processor must be specified")
	}

	fifos := make([]StageRunner, len(procs))
	for i, p := range procs {
		fifos[i] = FIFO(p)
	}

	return &broadcast{
		fifos: fifos,
	}
}

func (b *broadcast) Run(ctx context.Context, params StageParams) {
	var wg sync.WaitGroup
	var inCh = make([]chan Payload, len(b.fifos))
	for i := 0; i < len(b.fifos); i++ {
		wg.Add(1)
		inCh[i] = make(chan Payload)
		go func(fifoIndex int) {
			/*to avoid data races, the implementation for the broadcasting stage must intercept
			each incoming payload, clone it, and deliver a copy to each one of the generated FIFO
			processors*/
			fifoParams := &workerParams{
				stage: params.StageIndex(),
				inCh:  inCh[fifoIndex],
				outCh: params.Output(),
				errCh: params.Error(),
			}

			//the FIFOs must be wired to a dedicated input channel for reading, but they all share the same
			//output and error channels.  each FIFO needs its own input channel because of the cloning above^^
			b.fifos[fifoIndex].Run(ctx, fifoParams)
			wg.Done()
		}(i)
	}

done:
	for {
		//Read incoming payloads and pass them to each fifo
		select {
		case <-ctx.Done():
			break done
		case payload, ok := <-params.Input():
			if !ok {
				break done
			}

			//Clone payload and dispatch to each FIFO worker
			for i := len(b.fifos) - 1; i >= 0; i-- {
				var fifoPayload = payload
				if i != 0 {
					fifoPayload = payload.Clone()
				}

				select {
				case <-ctx.Done():
					break done
				case inCh[i] <- fifoPayload:
					//payload sent to the i_th FIFO
				}
			}
		}
	}

	//close input channels and wait for all FIFOs to exit
	for _, ch := range inCh {
		close(ch)
	}
	wg.Wait()
}
