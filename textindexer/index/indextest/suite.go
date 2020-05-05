package indextest

import (
	"time"

	"github.com/brandonshearin/ask_brandon/textindexer/index"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

/*
SuiteBase defines a re-usable set of index-realted tests that can
be executed against any type that implements index.Indexer (our in memory
bleve index implementaion, and our elasticsearch index)
*/
type SuiteBase struct {
	idx index.Indexer
}

/*
SetIndexer configures the test-suite to run all tests against the idx param.
Exactly like how we set the in memory link graph implementation
*/
func (s *SuiteBase) SetIndexer(idx index.Indexer) {
	s.idx = idx
}

//TestIndexDocument verifies the indexing logic for new documents
func (s *SuiteBase) TestIndexDocument(c *gc.C) {
	incompleteDoc := &index.Document{
		Content:  "Hello example content",
		Title:    "Title 1",
		URL:      "http://example.com",
		PageRank: 1,
	}

	err := s.idx.Index(incompleteDoc)
	c.Assert(err, gc.NotNil)
	c.Assert(xerrors.Is(err, index.ErrMissingLinkID), gc.Equals, true)

	id := uuid.New()
	doc := &index.Document{
		Content:   "Hello example content",
		Title:     "Title 1",
		URL:       "http://example.com",
		PageRank:  1,
		LinkID:    id,
		IndexedAt: time.Now().Add(-12 * time.Hour),
	}

	err = s.idx.Index(doc)
	c.Assert(err, gc.IsNil)
}
