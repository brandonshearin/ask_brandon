package memory

import (
	"github.com/blevesearch/bleve"
	"github.com/brandonshearin/ask_brandon/textindexer/index"
)

type bleveIterator struct {
	//allows iterator to access the stored docs when the iterator is advaned
	idx *InMemoryBleveIndexer
	//iterator needs a pointer to the sasrch request, to trigger new bleve searches once
	//the current page of results has been consumed
	searchReq *bleve.SearchRequest

	//counter that tracks the absolute position in the global result list
	cumIdx uint64
	//counter that tracks the position in the current page of results
	rsIdx int

	rs *bleve.SearchResult

	latchedDoc *index.Document
	lastErr    error
}

// Close the iterator and release any allocated resources.
func (it *bleveIterator) Close() error {
	it.idx = nil
	it.searchReq = nil
	if it.rs != nil {
		it.cumIdx = it.rs.Total
	}
	return nil
}

// Next loads the next document matching the search query.
// It returns false if no more documents are available.
func (it *bleveIterator) Next() bool {
	if it.lastErr != nil || it.rs == nil || it.cumIdx >= it.rs.Total {
		return false
	}

	// Do we need to fetch the next batch?
	if it.rsIdx >= it.rs.Hits.Len() {
		it.searchReq.From += it.searchReq.Size
		if it.rs, it.lastErr = it.idx.idx.Search(it.searchReq); it.lastErr != nil {
			return false
		}

		it.rsIdx = 0
	}

	nextID := it.rs.Hits[it.rsIdx].ID
	if it.latchedDoc, it.lastErr = it.idx.findByID(nextID); it.lastErr != nil {
		return false
	}

	it.cumIdx++
	it.rsIdx++
	return true
}

// Error returns the last error encountered by the iterator.
func (it *bleveIterator) Error() error {
	return it.lastErr
}

// Document returns the current document from the result set.
func (it *bleveIterator) Document() *index.Document {
	return it.latchedDoc
}

// TotalCount returns the approximate number of search results.
func (it *bleveIterator) TotalCount() uint64 {
	if it.rs == nil {
		return 0
	}
	return it.rs.Total
}
