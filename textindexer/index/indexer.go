package index

import "github.com/google/uuid"

/*
Indexer exposes an interface that can index and search documents
*/
type Indexer interface {
	/*
		Index adds a document to the index, or reindexes an existing document
		when its content changes.
	*/
	Index(doc *Document) error
	/*
		FindByID performs a lookup for a document by its ID
	*/
	FindByID(linkID uuid.UUID) (*Document, error)
	/*
		Search expects a Query type as opposed to a string argument.
		Offers us flexibility to expand the indexer's query capabilities
		further down the road without having to modify the Search() signature
	*/
	Search(query Query) (Iterator, error)
	/*
		UpdateScore updates the PageRank score for a document.
	*/
	UpdateScore(linkID uuid.UUID, score float64) error
}

//Query is an object that represents what our users search
type Query struct {
	/*
		Our indexer interprets expression strings in different ways.  Proof
		of concept will only implement two types of searches,
			(1) Search for a list of keywords in any order
			(2) Searching for an exact phrase match
	*/
	Type QueryType
	/*
		Expression stores the search query that's entered by the end user
		through a front end.
	*/
	Expression string
	// The number of serach results to skip
	Offset int
}

// QueryType describes the types of queries supported by the indexer implementations
type QueryType uint8

/*
These are the two types of search queries,
can be extended in the future to perform boolean-,
date-, or domain-based queries
*/
const (
	QueryTypeMatch QueryType = iota
	QueryTypePhrase
)

/*
Iterator is returned by search for a front end to
consume and paginate search results
*/
type Iterator interface {
	Close() error
	//Next loads the next document, returning false if no more available
	Next() bool
	//Error returns last error encountered by the iterator
	Error() error
	//Document returns the current document from the result set
	Document() *Document
	//TotalCount returns the approx. number of search results
	TotalCount() uint64
}
