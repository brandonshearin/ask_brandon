package index

import (
	"time"

	"github.com/google/uuid"
)

/*Document models an object that the indexer will index and search */
type Document struct {
	LinkID uuid.UUID

	URL string
	/*correspond to the value of the <title> element if the
	link points to an HTML page*/
	Title string
	/*stores the block of text extracted by the crawler*/
	Content string

	IndexedAt time.Time

	PageRank float64
}
