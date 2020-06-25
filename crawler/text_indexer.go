package crawler

import (
	"context"
	"time"

	"github.com/brandonshearin/ask_brandon/pipeline"
	"github.com/brandonshearin/ask_brandon/textindexer/index"
)

// Indexer is implemented by objects that can index the contents of webpages retrieved by the crawler pipeline
type Indexer interface {
	Index(doc *index.Document) error
}

type textIndexer struct {
	indexer Indexer
}

func newTextIndexer(indexer Indexer) *textIndexer {
	return &textIndexer{
		indexer: indexer,
	}
}

func (i *textIndexer) Process(ctx context.Context, p pipeline.Payload) (pipeline.Payload, error) {

	payload := p.(*crawlerPayload)

	doc := &index.Document{
		LinkID:    payload.LinkID,
		URL:       payload.URL,
		Title:     payload.Title,
		Content:   payload.TextContent,
		IndexedAt: time.Now(),
	}

	if err := i.indexer.Index(doc); err != nil {
		return nil, err
	}

	return p, nil
}
