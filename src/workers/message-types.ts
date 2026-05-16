// Shared message-type constants used by both the main-thread WorkerPool and
// the Worker entry-point. Extracted into its own module so the pool can import
// these constants WITHOUT evaluating worker-entry.ts (which throws if it's
// not running inside a worker thread).

export enum messageTypes {
    init,
    ready,
    task,
    result,
    error
}
