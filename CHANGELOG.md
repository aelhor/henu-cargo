# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-05-15

### Added

- `ingestStream()` — main function to reliably move data from a Readable stream to an IngestionSink with full lifecycle management (named **henu-cargo** after the sacred Nile cargo boat)
- `IngestionSink` interface — pluggable sink architecture with `write`, `finalize`, `abort` methods
- `IngestionOptions` — supports serial transforms, parallel worker-thread processing, AbortSignal cancellation, and `onProgress` callback
- `IngestionResult` — returns `totalBytes`, `duration`, `status`, `chunksProcessed`, `throughputBytesPerSec`
- `WorkerTransformFunction` type — contract for worker transform functions
- `createFsSink()` — built-in file system sink with backpressure via drain event
- Serial processing with optional inline transforms on the main thread
- Parallel processing with worker-thread pool, ordered Sequencer, and concurrency semaphore
- AbortSignal support — cancel ingestion mid-stream with automatic resource cleanup
- `onProgress` callback — observe `totalBytes`, `chunksProcessed`, `elapsedMs` during ingestion
- Guaranteed resource cleanup — source destroyed, workers terminated, signal listeners removed on every path (success, error, abort)
- Backpressure-aware writes — pauses source when sink is slow
- Mutual exclusion validation for `transform` and `parallel` options
- Zero runtime dependencies
- TypeScript type definitions shipped with the package
- Benchmark suite (`npm run bench`) — serial vs parallel throughput, memory stability, backpressure effectiveness, event loop responsiveness
- CI via GitHub Actions — builds and tests on Node 20
