package pipeline

import (
	"context"
	"fmt"

	gc "gopkg.in/check.v1"
)

/*==================================== source stub ====================================*/
type sourceStub struct {
	index int
	data  []Payload
	err   error
}

func (s *sourceStub) Next(ctx context.Context) bool {
	if s.err != nil || s.index == len(s.data) {
		return false
	}
	s.index++
	return true
}

func (s *sourceStub) Payload() Payload {
	return s.data[s.index-1]
}

func (s *sourceStub) Error() error {
	return s.err
}

/*==================================== sink stub ====================================*/
type sinkStub struct {
	data []Payload
	err  error
}

func (s *sinkStub) Consume(ctx context.Context, p Payload) error {
	s.data = append(s.data, p)
	return s.err
}

/*==================================== string payload ===============================*/
//stringPayload is a simple payload that has a string value
type stringPayload struct {
	processed bool
	val       string
}

func (s *stringPayload) Clone() Payload {
	return &stringPayload{val: s.val}
}

func (s *stringPayload) MarkAsProcessed() {
	s.processed = true
}

func (s *stringPayload) String() string {
	return s.val
}

//generate many string payloads
func stringPayloads(numValues int) []Payload {
	out := make([]Payload, numValues)
	for i := 0; i < len(out); i++ {
		out[i] = &stringPayload{val: fmt.Sprint(i)}
	}
	return out
}

/*==================================== test stage ==================================*/

type testStage struct {
	c            *gc.C
	dropPayloads bool
	err          error
}

func (s testStage) Run(ctx context.Context, params StageParams) {
	defer func() {
		s.c.Logf("[stage %d] exiting", params.StageIndex())
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-params.Input():
			if !ok {
				return
			}
			s.c.Logf("[stage %d] received payload: %v", params.StageIndex(), p)
			if s.err != nil {
				s.c.Logf("[stage %d] emit error: %v", params.StageIndex(), s.err)
				params.Error() <- s.err
				return
			}

			if s.dropPayloads {
				s.c.Logf("[stage %d] dropping payload: %v", params.StageIndex(), p)
				p.MarkAsProcessed()
				continue
			}

			s.c.Logf("[stage %d] emitting payload: %v", params.StageIndex(), p)
			select {
			case <-ctx.Done():
				return
			case params.Output() <- p:
			}
		}
	}
}
