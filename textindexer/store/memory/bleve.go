package memory

import (
	"sync"
	"time"

	"github.com/blevesearch/bleve"
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

func (i *InMemoryBleveIndexer) FindById(linkID uuid.UUID) (*index.Document, error) {
	return nil, nil
}

func (i *InMemoryBleveIndexer) Search(query index.Query) (index.Iterator, error) {
	return nil, nil
}

func (i *InMemoryBleveIndexer) UpdateScore(linkID uuid.UUID, score float64) error {
	return nil
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
