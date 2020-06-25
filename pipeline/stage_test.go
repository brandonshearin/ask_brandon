package pipeline

import (
	"context"
	"testing"

	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(StageTestSuite))

type StageTestSuite struct{}

func Test(t *testing.T) { gc.TestingT(t) }

func (s StageTestSuite) TestFIFO(c *gc.C) {
	stages := make([]StageRunner, 10)
	for i := 0; i < len(stages); i++ {
		stages[i] = FIFO(makePassthroughProcessor())
	}

	src := &sourceStub{data: stringPayloads(3)}
	sink := new(sinkStub)

	p := New(stages...)
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, gc.IsNil)
	c.Assert(sink.data, gc.DeepEquals, src.data)
	assertAllProcessed(c, src.data)
}

//passes payload through to next stage
func makePassthroughProcessor() Processor {
	return ProcessorFunc(func(_ context.Context, p Payload) (Payload, error) {
		return p, nil
	})
}

func assertAllProcessed(c *gc.C, payloads []Payload) {
	for i, p := range payloads {
		payload := p.(*stringPayload)
		c.Assert(payload.processed, gc.Equals, true, gc.Commentf("payload %d not processed", i))
	}
}
