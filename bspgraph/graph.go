package bspgraph

import (
	"sync"
	"sync/atomic"

	"github.com/brandonshearin/ask_brandon/bspgraph/message"
	"golang.org/x/xerrors"
)

var (
	// ErrUnknownEdgeSource is returned by AddEdge when the source vertex
	// is not present in the graph.
	ErrUnknownEdgeSource = xerrors.New("source vertex is not part of the graph")

	// ErrDestinationIsLocal is returned by Relayer instances to indicate
	// that a message destination is actually owned by the local graph.
	ErrDestinationIsLocal = xerrors.New("message destination is assigned to the local graph")

	// ErrInvalidMessageDestination is returned by calls to SendMessage and
	// BroadcastToNeighbors when the destination cannot be resolved to any
	// (local or remote) vertex.
	ErrInvalidMessageDestination = xerrors.New("invalid message destination")
)

type Vertex struct {
	id       string
	value    interface{}
	active   bool
	msgQueue [2]message.Queue
	edges    []*Edge
}

func (v *Vertex) ID() string { return v.id }

func (v *Vertex) Value() interface{} { return v.value }

func (v *Vertex) SetValue(val interface{}) { v.value = val }

func (v *Vertex) Freeze() { v.active = false }

func (v *Vertex) Edges() []*Edge { return v.edges }

type Edge struct {
	value interface{}
	dstID string
}

func (e *Edge) DstID() string { return e.dstID }

func (e *Edge) Value() interface{} { return e.value }

func (e *Edge) SetValue(val interface{}) { e.value = val }

// Graph implements a parallel graph processor based on the concepts described
// in the Pregel paper.
type Graph struct {
	superstep int

	aggregators map[string]Aggregator
	vertices    map[string]*Vertex
	computeFn   ComputeFunc

	queueFactory message.QueueFactory
	relayer      Relayer

	wg              sync.WaitGroup
	vertexCh        chan *Vertex
	errCh           chan error
	stepCompletedCh chan struct{}
	activeInStep    int64
	pendingInStep   int64
}

func NewGraph(cfg GraphConfig) (*Graph, error) {
	if err := cfg.validate(); err != nil {
		return nil, xerrors.Errorf("graph config validation failed: %w", err)
	}

	g := &Graph{
		computeFn:    cfg.ComputeFn,
		queueFactory: cfg.QueueFactory,
		aggregators:  make(map[string]Aggregator),
		vertices:     make(map[string]*Vertex),
	}

	g.startWorkers(cfg.ComputeWorkers)

	return g, nil
}

func (g *Graph) Reset() error {
	g.superstep = 0
	for _, v := range g.vertices {
		for i := 0; i < 2; i++ {
			if err := v.msgQueue[i].Close(); err != nil {
				return xerrors.Errorf("closing message queue #%d for vertex %v: %w", i, v.ID(), err)
			}
		}
	}

	g.vertices = make(map[string]*Vertex)
	g.aggregators = make(map[string]Aggregator)
	return nil
}

func (g *Graph) Close() error {
	close(g.vertexCh)
	g.wg.Wait()

	return g.Reset()
}

// AddVertex inserts a new vertex with the specified id and initial value
// into the graph.  If the vertex already exists, AddVertex will just overwrite
// its value with the provided initValue
func (g *Graph) AddVertex(id string, initValue interface{}) {
	v := g.vertices[id]
	// if vertex not in graph, create & add
	if v == nil {
		v = &Vertex{
			id: id,
			msgQueue: [2]message.Queue{
				g.queueFactory(),
				g.queueFactory(),
			},
			active: true,
		}
		g.vertices[id] = v
	}
	// else update existing value
	v.SetValue(initValue)
}

// AddEdge inserts a directed edge from src to destination and annotates it with the specified initValue.
// By design, edges are owned by the source vertices (destinations can be either local or remote)
// and therefore srcID must resolve to a local vertex.  Otherwise, AddEdge returns an error
func (g *Graph) AddEdge(srcID, dstID string, initValue interface{}) error {
	srcVert := g.vertices[srcID]
	if srcVert == nil {
		return xerrors.Errorf("create edge from %q to %q: %w", srcID, dstID, ErrUnknownEdgeSource)
	}

	srcVert.edges = append(srcVert.edges, &Edge{
		dstID: dstID,
		value: initValue,
	})

	return nil
}

// Superstep returns the current superstep value.
func (g *Graph) Superstep() int { return g.superstep }

// RegisterAggregator adds an aggregator with the specified name into the graph
func (g *Graph) RegisterAggregator(name string, aggr Aggregator) {
	g.aggregators[name] = aggr
}

// Aggregator returns the aggregator with the specified name or nil
func (g *Graph) Aggregator(name string) Aggregator {
	return g.aggregators[name]
}

// Aggregators returns a map of all currently registered aggregators where the
// key is the aggregator's name.
func (g *Graph) Aggregators() map[string]Aggregator { return g.aggregators }

// BroadcastToNeighbors is a helper function that broadcasts a single message
// to each neighbor of a particular vertex.  Messages are queued for delivery
// and will be processed by recipients in the next superstep
func (g *Graph) BroadcastToNeighbors(v *Vertex, msg message.Message) error {
	for _, e := range v.edges {
		if err := g.SendMessage(e.dstID, msg); err != nil {
			return err
		}
	}

	return nil
}

func (g *Graph) SendMessage(dstID string, msg message.Message) error {
	dstVert := g.vertices[dstID]
	if dstVert != nil {
		queueIndex := (g.superstep + 1) % 2
		return dstVert.msgQueue[queueIndex].Enqueue(msg)
	}

	if g.relayer != nil {
		if err := g.relayer.Relay(dstID, msg); !xerrors.Is(err, ErrDestinationIsLocal) {
			return err
		}
	}

	return xerrors.Errorf("message cannot be delivered to %q: %w", dstID, ErrInvalidMessageDestination)
}

// startWorkers allocates the required channels and spins up numWorkers to
// execute each superstep
func (g *Graph) startWorkers(numWorkers int) {
	g.vertexCh = make(chan *Vertex)
	g.errCh = make(chan error, 1)
	g.stepCompletedCh = make(chan struct{})

	g.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go g.stepWorker()
	}
}

// Step executes the next superstep and returns back the number of vertices that
// we processed either bcause they were still active or because they receieved a message
func (g *Graph) step() (activeInStep int, err error) {
	g.activeInStep, g.pendingInStep = 0, int64(len(g.vertices))
	if g.pendingInStep == 0 {
		return 0, nil //no work required
	}

	for _, v := range g.vertices {
		g.vertexCh <- v
	}

	// wait for all vertices to be processed by the worker pool
	// by performing a blocking read on stepCompletedCh
	<-g.stepCompletedCh

	select {
	case err = <-g.errCh: // dequeued
	default: // no error available
	}

	return int(g.activeInStep), err
}

// stepWorker polls vertexCh for incoming vertices and executes the configured
// ComputeFunc for each one.  The worker automatically exits when vertexCh
// gets closed
func (g *Graph) stepWorker() {
	for v := range g.vertexCh {
		buffer := g.superstep % 2
		if v.active || v.msgQueue[buffer].PendingMessages() {
			_ = atomic.AddInt64(&g.activeInStep, 1)
			v.active = true
			if err := g.computeFn(g, v, v.msgQueue[buffer].Messages()); err != nil {
				tryEmitError(g.errCh, xerrors.Errorf("running compute function for vertex %q failed: %w", v.ID(), err))
			} else if err := v.msgQueue[buffer].DiscardMessages(); err != nil {
				tryEmitError(g.errCh, xerrors.Errorf("discarding unprocessed messages for vertex %q failed: %w", v.ID(), err))
			}
		}
		if atomic.AddInt64(&g.pendingInStep, -1) == 0 {
			g.stepCompletedCh <- struct{}{}
		}
	}

	g.wg.Done()
}

func tryEmitError(errCh chan<- error, err error) {
	select {
	case errCh <- err: // queued error
	default: // channel already contains another error
	}
}
