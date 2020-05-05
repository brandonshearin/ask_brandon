package memory

import (
	"testing"

	"github.com/brandonshearin/ask_brandon/textindexer/index/indextest"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(InMemoryBleveTestSuite))

type InMemoryBleveTestSuite struct {
	indextest.SuiteBase
	idx *InMemoryBleveIndexer
}

func Test(t *testing.T) { gc.TestingT(t) }

func (s *InMemoryBleveTestSuite) SetUpTest(c *gc.C) {
	idx, err := NewInMemoryBleveIndexer()
	c.Assert(err, gc.IsNil)
	s.SetIndexer(idx)
	s.idx = idx
}

func (s *InMemoryBleveTestSuite) TearDownTest(c *gc.C) {
	c.Assert(s.idx.Close(), gc.IsNil)
}
