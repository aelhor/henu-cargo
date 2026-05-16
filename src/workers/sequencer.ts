// Reorder buffer for out-of-order worker results.
//
// Workers complete in arbitrary order, but the sink must receive chunks in
// the original read order. The Sequencer buffers arrived results and provides
// a `waitFor(id)` method that blocks until the chunk with that ID is ready.
//
// Two cases:
//   1. insert(id) called BEFORE waitFor(id) — chunk sits in the buffer until
//      waitFor(id) picks it up.
//   2. waitFor(id) called BEFORE insert(id) — a promise is created and stored
//      in `waiters`; insert resolves it when the chunk arrives.

export class Sequencer {
    private buffer: Map<number, Buffer | null> = new Map()
    private waiters: Map<number, (value: Buffer | null) => void> = new Map()

    /**
     * Called by the pool.onResult callback when a worker finishes.
     * Stores the chunk. If a drain-loop waiter already exists for this ID,
     * resolves it immediately.
     */
    insert(sequenceId: number, chunk: Buffer | null) {
        // If someone is already waiting for this exact ID, resolve them now.
        const waiter = this.waiters.get(sequenceId);
        if (waiter) {
            this.waiters.delete(sequenceId);
            waiter(chunk);
        } else {
            // No waiter yet — buffer the result until waitFor() asks for it.
            this.buffer.set(sequenceId, chunk);
        }
    }

    /**
     * Called by the drain loop to get the next result in order.
     * Returns immediately if the result is already buffered, otherwise
     * returns a Promise that resolves when insert() receives that ID.
     */
    waitFor(sequenceId: number): Promise<Buffer | null> {
        if (this.buffer.has(sequenceId)) {
            const chunk = this.buffer.get(sequenceId)!;
            this.buffer.delete(sequenceId);
            return Promise.resolve(chunk);
        }

        return new Promise<Buffer | null>(resolve => {
            this.waiters.set(sequenceId, resolve);
        });
    }
}
