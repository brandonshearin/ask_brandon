package message

import "sync"

// inMemoryQueue implmements a queue that stores messages in memory.  Messages
// can be enqueued concurrently but the returned iterator is not safe for
// concurrent access
type inMemoryQueue struct {
	mu   sync.Mutex
	msgs []Message

	latchedMsg Message
}

// NewInMemoryQueue creates a new in-memory queue instance.  This function can
// serve as a QueueFactory
func NewInMemoryQueue() Queue {
	return new(inMemoryQueue)
}

func (q *inMemoryQueue) Enqueue(msg Message) error {
	q.mu.Lock()
	q.msgs = append(q.msgs, msg)
	q.mu.Unlock()
	return nil
}

func (q *inMemoryQueue) PendingMessages() bool {
	q.mu.Lock()
	pending := len(q.msgs) != 0
	q.mu.Unlock()

	return pending
}

func (q *inMemoryQueue) DiscardMessages() error {
	q.mu.Lock()
	q.msgs = q.msgs[:0]
	q.mu.Unlock()
	return nil
}

func (q *inMemoryQueue) Close() error { return nil }

func (q *inMemoryQueue) Messages() Iterator { return q }

// Next uses LIFO semantics for dequeueing.  That is intentional because
// dequeueing from the head of the list would decrease the capacity  of the slice,
// and we wouldn't be able to reuse the already allocated memory to append new messages in the future
func (q *inMemoryQueue) Next() bool {
	q.mu.Lock()
	qLen := len(q.msgs)
	if qLen == 0 {
		q.mu.Unlock()
		return false
	}
	// Dequeue message from the tail of the queue.
	q.latchedMsg = q.msgs[qLen-1]
	q.msgs = q.msgs[:qLen-1]
	q.mu.Unlock()
	return true
}

func (q *inMemoryQueue) Message() Message {
	q.mu.Lock()
	msg := q.latchedMsg
	q.mu.Unlock()
	return msg
}

func (q *inMemoryQueue) Error() error { return nil }
