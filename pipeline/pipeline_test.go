package pipeline

import (
	"context"

	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(PipelineTestSuite))

type PipelineTestSuite struct{}

func (s *PipelineTestSuite) TestDataFlow(c *gc.C) {
	stages := make([]StageRunner, 10)
	for i := 0; i < len(stages); i++ {
		stages[i] = testStage{c: c}
	}

	src := &sourceStub{data: stringPayloads(3)}
	sink := new(sinkStub)

	p := New(stages...)
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, gc.IsNil)
	c.Assert(sink.data, gc.DeepEquals, src.data)
	assertAllProcessed(c, src.data)
}

func (s *PipelineTestSuite) TestProcessorErrorHandling(c *gc.C) {
	expErr := xerrors.New("some error")
	stages := make([]StageRunner, 10)
	for i := 0; i < len(stages); i++ {
		var err error
		if i == 5 {
			err = expErr
		}

		stages[i] = testStage{c: c, err: err}
	}

	src := &sourceStub{data: stringPayloads(3)}
	sink := new(sinkStub)

	p := New(stages...)
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, gc.ErrorMatches, "(?s).*some error.*")
}

func (s *PipelineTestSuite) TestSourceErrorHandling(c *gc.C) {
	expErr := xerrors.New("some error")
	src := &sourceStub{err: expErr, data: stringPayloads(3)}
	sink := new(sinkStub)

	p := New(testStage{c: c})
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, gc.ErrorMatches, "*pipeline source: some error.*")
}

func (s *PipelineTestSuite) TestSinkErrorHandling(c *gc.C) {
	expErr := xerrors.New("some error")
	src := &sourceStub{data: stringPayloads(3)}
	sink := &sinkStub{err: expErr}

	p := New(testStage{c: c})
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, gc.ErrorMatches, "*pipeline sink: some error.*")
}

func (s *PipelineTestSuite) TestPayloadDiscarding(c *gc.C) {
	src := &sourceStub{data: stringPayloads(3)}
	sink := &sinkStub{}

	p := New(testStage{c: c, dropPayloads: true})
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, gc.IsNil)
	c.Assert(sink.data, gc.HasLen, 0, gc.Commentf("expected all payloads to be discarded by stage processor"))
	assertAllProcessed(c, src.data)
}
