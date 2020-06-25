package crawler

import (
	"context"
	"time"

	"github.com/brandonshearin/ask_brandon/linkgraph/graph"
	"github.com/brandonshearin/ask_brandon/pipeline"
	"github.com/google/uuid"
)

type graphUpdater struct {
	updater Graph
}

func newGraphUpdater(updater Graph) *graphUpdater {
	return &graphUpdater{
		updater: updater,
	}
}

// Graph is a subset of the methods exposed by our linkgraph module.
// A good example of the interface-segregation principle
type Graph interface {
	UpsertLink(link *graph.Link) error
	UpsertEdge(edge *graph.Edge) error
	RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error
}

func (u *graphUpdater) Process(ctx context.Context, p pipeline.Payload) (pipeline.Payload, error) {

	payload := p.(*crawlerPayload)

	src := &graph.Link{
		ID:          payload.LinkID,
		URL:         payload.URL,
		RetrievedAt: time.Now(),
	}

	if err := u.updater.UpsertLink(src); err != nil {
		return nil, err
	}

	for _, dstLink := range payload.NoFollowLinks {
		dst := &graph.Link{URL: dstLink}
		if err := u.updater.UpsertLink(dst); err != nil {
			return nil, err
		}
	}

	removeEdgesOlderThan := time.Now()
	for _, dstLink := range payload.Links {
		dst := &graph.Link{URL: dstLink}

		if err := u.updater.UpsertLink(dst); err != nil {
			return nil, err
		}

		if err := u.updater.UpsertEdge(&graph.Edge{Src: src.ID, Dst: dst.ID}); err != nil {
			return nil, err
		}

		if err := u.updater.RemoveStaleEdges(src.ID, removeEdgesOlderThan); err != nil {
			return nil, err
		}
	}

	return p, nil
}
