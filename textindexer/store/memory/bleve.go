package memory

import (
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/brandonshearin/ask_brandon/textindexer/index"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

/*
InMemoryBleveIndexer implements an in memory index.  Bleve is primarily designed to store its index
on disk, but it also support an in-memory index.  This makes it good for running unit tests
in isolation or for demo purposes.
*/
type InMemoryBleveIndexer struct {
	//mu ensures that the in-memory indexer is safe for concurrent use
	mu   sync.RWMutex
	docs map[string]*index.Document
	//idx stores a reference to the bleve index
	idx bleve.Index
}

/*
bleveDoc is the object bleve indexes for us for full-text searching
*/
type bleveDoc struct {
	Title    string
	Content  string
	PageRank float64
}

//NewInMemoryBleveIndexer creates a text indexer that uses an in-memory bleve instance for indexing docs
func NewInMemoryBleveIndexer() (*InMemoryBleveIndexer, error) {
	mapping := bleve.NewIndexMapping()
	idx, err := bleve.NewMemOnly(mapping)
	if err != nil {
		return nil, err
	}

	return &InMemoryBleveIndexer{
		idx:  idx,
		docs: make(map[string]*index.Document),
	}, nil
}

// Close the indexer and release any allocated resources.
func (i *InMemoryBleveIndexer) Close() error {
	return i.idx.Close()
}

/*
Index stores a light-weight version of our document object into the bleve in-memory store.
*/
func (i *InMemoryBleveIndexer) Index(doc *index.Document) error {
	if doc.LinkID == uuid.Nil {
		return xerrors.Errorf("index: %w", index.ErrMissingLinkID)
	}
	doc.IndexedAt = time.Now()
	dcopy := copyDoc(doc)
	key := dcopy.LinkID.String()
	//acquire write lock when making changes to data structure
	i.mu.Lock()
	/*if doc has already been indexed, copy over its PageRank value*/
	if orig, exists := i.docs[key]; exists {
		dcopy.PageRank = orig.PageRank
	}

	if err := i.idx.Index(key, makeBleveDoc(dcopy)); err != nil {
		return xerrors.Errorf("index: %w", err)
	}
	i.docs[key] = dcopy
	i.mu.Unlock()
	return nil
}

/*
FindByID converts the input uuid to a string and delegates document lookup to the unexported findByID method.
This is because we need to provide a string-based ID for bleve to index a document, which bleve returns to us
when the document is matched by a search query.  By providing a findByID method that accepts linkID as a string, we
can reuse the document lookup code when iterating search results
*/
func (i *InMemoryBleveIndexer) FindByID(linkID uuid.UUID) (*index.Document, error) {
	return i.findByID(linkID.String())
}

//Search is called by clients of the text indexer to submit queries
func (i *InMemoryBleveIndexer) Search(q index.Query) (index.Iterator, error) {
	//Determine what type of query the caller asked us to perform,
	//invoking the appropriate bleve helper
	var bq query.Query
	switch q.Type {
	case index.QueryTypePhrase:
		bq = bleve.NewMatchPhraseQuery(q.Expression)
	case index.QueryTypeMatch:
		bq = bleve.NewMatchQuery(q.Expression)
	}

	searchReq := bleve.NewSearchRequest(bq)
	searchReq.SortBy([]string{"-PageRank", "-_score"})
	searchReq.Size = 10
	searchReq.From = q.Offset
	rs, err := i.idx.Search(searchReq)
	if err != nil {
		return nil, xerrors.Errorf("search: %w", err)
	}
	//if the search returns a result, present an iterator to the caller for them to consume the matched documents
	return &bleveIterator{idx: i, searchReq: searchReq, rs: rs, cumIdx: uint64(q.Offset)}, nil
}

/*
UpdateScore will update pagerank score of the document with linkID in place, after acquiring write lock.
*/
func (i *InMemoryBleveIndexer) UpdateScore(linkID uuid.UUID, score float64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	key := linkID.String()
	if doc, found := i.docs[key]; found {
		//any updates to a searchable attribute requires a reindex operation.
		//PageRank of document is updated in-place since we have acquired a write lock
		doc.PageRank = score
		if err := i.idx.Index(key, makeBleveDoc(doc)); err != nil {
			return xerrors.Errorf("update score: %w", err)
		}
	} else {
		//if document not found, don't index it but still store it
		doc := &index.Document{LinkID: linkID, PageRank: score}
		i.docs[key] = doc
	}
	return nil
}

func (i *InMemoryBleveIndexer) findByID(linkID string) (*index.Document, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if doc, found := i.docs[linkID]; found {
		return copyDoc(doc), nil
	}
	return nil, xerrors.Errorf("find by ID: %w", index.ErrNotFound)
}

func copyDoc(d *index.Document) *index.Document {
	dCopy := new(index.Document)
	*dCopy = *d
	return dCopy
}

/*
makeBleveDoc helper returns a partial, light weight view of the original document
that contains only the fields we want to use as part of our search queries
*/
func makeBleveDoc(d *index.Document) bleveDoc {
	return bleveDoc{
		Title:    d.Title,
		Content:  d.Content,
		PageRank: d.PageRank,
	}
}
