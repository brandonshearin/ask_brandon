package indextest

import (
	"fmt"
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

//TestFindByID verifies the document lookup logic
func (s *SuiteBase) TestFindByID(c *gc.C) {
	doc := &index.Document{
		LinkID:    uuid.New(),
		Content:   "hello",
		IndexedAt: time.Now(),
		PageRank:  1,
		Title:     "Title",
		URL:       "http://example.com",
	}
	err := s.idx.Index(doc)
	c.Assert(err, gc.IsNil)

	got, err := s.idx.FindByID(doc.LinkID)
	c.Assert(err, gc.IsNil)
	c.Assert(got, gc.DeepEquals, doc, gc.Commentf("document returned from FindByID does not match inserted document"))

	got, err = s.idx.FindByID(uuid.New())
	c.Assert(got, gc.IsNil)
	c.Assert(xerrors.Is(err, index.ErrNotFound), gc.Equals, true)
}

//TestUpdateScore1 verifies that a document's pagerank is changed correctly
func (s *SuiteBase) TestUpdateScore1(c *gc.C) {
	doc := &index.Document{
		LinkID:   uuid.New(),
		PageRank: 1,
	}
	err := s.idx.Index(doc)
	c.Assert(err, gc.IsNil)

	err = s.idx.UpdateScore(doc.LinkID, float64(5))
	c.Assert(err, gc.IsNil)
	got, err := s.idx.FindByID(doc.LinkID)
	c.Assert(err, gc.IsNil)
	c.Assert(got.PageRank, gc.Equals, float64(5), gc.Commentf("PageRank score not updated"))
}

//TestUpdateScoreUnknownDocument verifies that PageRank score is updated on documents that aren't indexed
func (s *SuiteBase) TestUpdateScoreUnknownDocument(c *gc.C) {
	id := uuid.New()
	err := s.idx.UpdateScore(id, float64(10))
	c.Assert(err, gc.IsNil)
	found, err := s.idx.FindByID(id)
	c.Assert(err, gc.IsNil)

	c.Assert(found.URL, gc.Equals, "")
	c.Assert(found.Title, gc.Equals, "")
	c.Assert(found.Content, gc.Equals, "")
	c.Assert(found.PageRank, gc.Equals, float64(10))

}

func (s *SuiteBase) iterateDocs(c *gc.C, it index.Iterator) []uuid.UUID {
	var seen []uuid.UUID
	for it.Next() {
		seen = append(seen, it.Document().LinkID)
	}
	c.Assert(it.Error(), gc.IsNil)
	c.Assert(it.Close(), gc.IsNil)
	return seen
}

func (s *SuiteBase) reverse(collection []uuid.UUID) []uuid.UUID {
	for i, j := 0, len(collection)-1; i < j; i, j = i+1, j-1 {
		collection[i], collection[j] = collection[j], collection[i]
	}
	return collection
}

//TestUpdateScore2 verifies that scores are updated by iterating over a bleve search result
func (s *SuiteBase) TestUpdateScore2(c *gc.C) {
	var (
		numDocs     = 100
		expectedIDs []uuid.UUID
	)

	for i := 0; i < numDocs; i++ {
		id := uuid.New()
		expectedIDs = append(expectedIDs, id)
		doc := &index.Document{
			LinkID:  id,
			Title:   fmt.Sprintf("doc with ID %s", id.String()),
			Content: fmt.Sprintf("this is a test document"),
		}
		//index new document
		err := s.idx.Index(doc)
		c.Assert(err, gc.IsNil)

		//update the score of the new document
		err = s.idx.UpdateScore(id, float64(numDocs-i))
		c.Assert(err, gc.IsNil)
	}
	it, err := s.idx.Search(index.Query{
		Type:       index.QueryTypeMatch,
		Expression: "test",
	})
	c.Assert(err, gc.IsNil)
	c.Assert(s.iterateDocs(c, it), gc.DeepEquals, expectedIDs)

	// Update the pagerank scores so that results are sorted in the
	// reverse order.
	for i := 0; i < numDocs; i++ {
		err = s.idx.UpdateScore(expectedIDs[i], float64(i))
		c.Assert(err, gc.IsNil, gc.Commentf(expectedIDs[i].String()))
	}

	it, err = s.idx.Search(index.Query{
		Type:       index.QueryTypeMatch,
		Expression: "test",
	})
	c.Assert(err, gc.IsNil)
	c.Assert(s.iterateDocs(c, it), gc.DeepEquals, s.reverse(expectedIDs))
}

//TestPhraseSearch verifies the document search logic when searching for exact phrases
func (s *SuiteBase) TestPhraseSearch(c *gc.C) {
	var (
		numDocs     = 100
		expectedIDs []uuid.UUID
	)
	for i := 0; i < numDocs; i++ {
		id := uuid.New()
		doc := &index.Document{
			LinkID:  id,
			Title:   fmt.Sprintf("Doc with id %s", id.String()),
			Content: "One Two Three",
		}

		if i%5 == 0 {
			doc.Content = "Three Two One"
			expectedIDs = append(expectedIDs, id)
		}

		err := s.idx.Index(doc)
		c.Assert(err, gc.IsNil)

		err = s.idx.UpdateScore(id, float64(numDocs-i))
	}
	//construct a query for exact phrases
	it, err := s.idx.Search(index.Query{
		Type:       index.QueryTypePhrase,
		Expression: "three two one",
	})
	c.Assert(err, gc.IsNil)
	c.Assert(s.iterateDocs(c, it), gc.DeepEquals, expectedIDs)
}

//TestMatchSearch verifies the document search logic when searching for keyword matches
func (s *SuiteBase) TestMatchSearch(c *gc.C) {
	var (
		numDocs     = 100
		expectedIDs []uuid.UUID
	)

	for i := 0; i < numDocs; i++ {
		id := uuid.New()
		doc := &index.Document{
			LinkID:  id,
			Title:   fmt.Sprintf("Document with id %s", id.String()),
			Content: "this is the text of a document",
		}

		if i%5 == 0 {
			doc.Content = "this content is interesting"
			expectedIDs = append(expectedIDs, id)
		}

		err := s.idx.Index(doc)
		c.Assert(err, gc.IsNil)
		//we need to articially invert the score (numDocs - i) because
		//when we need the expected IDs to be in descending order to match
		//the iterator returned by calls to Search()
		err = s.idx.UpdateScore(id, float64(numDocs-i))
	}

	it, err := s.idx.Search(index.Query{
		Type:       index.QueryTypeMatch,
		Expression: "interesting content",
	})
	c.Assert(err, gc.IsNil)
	c.Assert(s.iterateDocs(c, it), gc.DeepEquals, expectedIDs)
}
