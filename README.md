# henu-cargo

[![CI](https://github.com/aelhor/henu-cargo/actions/workflows/ci.yml/badge.svg)](https://github.com/aelhor/henu-cargo/actions/workflows/ci.yml)

Named after the sacred Nile cargo boat of ancient Egypt, **henu-cargo** carries your data streams safely from source to sink — backpressure, resource cleanup, cancellation, and ordered parallel processing via worker threads, all with zero dependencies.

Pluggable sink architecture, AbortSignal support, constant memory, zero dependencies. Built for ETL, file processing, and data-heavy workloads.

## Install

```bash
npm install henu-cargo
```

## Quick Start

```typescript
import { ingestStream, createFsSink } from 'henu-cargo';
import { createReadStream } from 'node:fs';

const source = createReadStream('large-file.bin');
const sink = createFsSink('output.bin');

const result = await ingestStream(source, sink);

console.log(`Processed ${result.totalBytes} bytes in ${result.duration}ms`);
// Processed 104857600 bytes in 342ms
```

## Why This Exists

Every Node.js project that handles large streams eventually writes the same messy boilerplate:

- Forgetting to handle stream errors → uncaught exceptions
- Writing chunks without awaiting drain → memory blows up
- No backpressure → process crashes on large files
- Leaking resources on failure → production memory leaks
- CPU-heavy transforms block the event loop → server becomes unresponsive

**henu-cargo** solves all of this in a single function call. It gives you a constant memory footprint regardless of file size, proper resource cleanup on every path (success, error, abort), and optional worker-thread parallelism that preserves chunk order.

## Features

- **Backpressure-aware** — pauses the source when the sink is slow, prevents heap exhaustion
- **Worker-thread parallelism** — offload CPU-heavy transforms to a worker pool with guaranteed chunk ordering
- **Sink-agnostic** — implement a simple 3-method interface (`write`, `finalize`, `abort`) to write anywhere: files, databases, S3, HTTP
- **AbortSignal support** — cancel ingestion mid-stream with standard `AbortController`
- **Full resource cleanup** — source streams destroyed, workers terminated, no dangling listeners
- **Zero runtime dependencies** — built entirely on Node.js core modules
- **TypeScript-first** — full type definitions shipped with the package

## Usage

### Serial Processing with Transform

Process each chunk on the main thread, one at a time:

```typescript
import { ingestStream } from 'henu-cargo';
import { createReadStream } from 'node:fs';

const sink = {
  write: async (chunk: Buffer) => {
    await database.insert(chunk);
  },
  finalize: async () => {
    await database.flush();
  },
  abort: async (error: Error) => {
    await database.rollback();
  }
};

const result = await ingestStream(createReadStream('data.csv'), sink, {
  transform: async (chunk: Buffer) => {
    const parsed = parseCSV(chunk);
    return Buffer.from(JSON.stringify(parsed));
  }
});
```

### Parallel Processing with Worker Threads

Offload CPU-heavy transforms to a worker pool. Chunks are processed out of order by workers but **written to the sink in the original order**:

```typescript
import { ingestStream } from 'henu-cargo';
import path from 'node:path';
import os from 'node:os';

const result = await ingestStream(source, sink, {
  parallel: {
    workerPath: path.resolve(__dirname, 'my-transform-worker.js'),
    poolSize: Math.max(2, os.cpus().length - 2),
    concurrency: 16 // optional: max in-flight chunks (default: poolSize * 2)
  }
});
```

**Step 1:** Create a worker transform file. This is a separate `.js` or compiled `.ts` file that exports a default function:

```javascript
// my-transform-worker.js
module.exports.default = async function (chunk) {
  // Your CPU-heavy logic here: compression, encryption, parsing, hashing
  const compressed = await compress(chunk);
  return compressed;
};
```

**TypeScript worker:**

```typescript
// my-transform-worker.ts
import { WorkerTransformFunction } from 'henu-cargo';

const myTransform: WorkerTransformFunction = async (chunk) => {
  return chunk;
};

export default myTransform;
```

The worker file is loaded once per worker at startup. Each chunk is dispatched to an available worker, and results are re-ordered before writing to the sink.

### Cancellation with AbortSignal

Cancel ingestion at any time using the standard `AbortController`:

```typescript
const controller = new AbortController();

// Cancel after 30 seconds
setTimeout(() => controller.abort('Upload timed out'), 30_000);

const result = await ingestStream(source, sink, {
  signal: controller.signal
});

if (result.status === 'aborted') {
  console.log('Cancelled:', result.error?.message);
}
```

On abort: the source stream is destroyed, the sink's `abort()` is called for cleanup, and all workers are terminated. No dangling resources.

### Custom Sinks

Implement the `IngestionSink` interface to write anywhere:

```typescript
import { IngestionSink } from 'henu-cargo';

const s3Sink: IngestionSink = {
  async write(chunk: Buffer) {
    await s3.uploadPart({ Bucket: 'my-bucket', Body: chunk });
  },
  async finalize() {
    await s3.completeMultipartUpload({ Bucket: 'my-bucket' });
  },
  async abort(error: Error) {
    await s3.abortMultipartUpload({ Bucket: 'my-bucket' });
  }
};
```

### Built-in File Sink

```typescript
import { createFsSink } from 'henu-cargo';

const sink = createFsSink('/path/to/output.bin');
```

## API Reference

### `ingestStream(source, sink, options?)`

Main function. Reads a stream and writes to a sink with full lifecycle management.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `source` | `Readable` | Any Node.js readable stream |
| `sink` | `IngestionSink` | Object implementing `write`, `finalize`, `abort` |
| `options?` | `IngestionOptions` | Optional configuration |

**Returns:** `Promise<IngestionResult>`

### `IngestionSink`

```typescript
interface IngestionSink {
  write(chunk: Buffer): Promise<void>;
  finalize(): Promise<void>;
  abort(error: Error): Promise<void>;
}
```

- `write` — called for each chunk. Backpressure is applied: the next chunk waits until this resolves.
- `finalize` — called once after all chunks are written successfully.
- `abort` — called once on error or cancellation. Use for cleanup (rolling back transactions, deleting temp files, etc.).

### `IngestionOptions`

```typescript
interface IngestionOptions {
  // Serial: transform each chunk on the main thread
  transform?: (chunk: Buffer) => Buffer | Promise<Buffer | null> | null;

  // Parallel: dispatch to worker threads
  parallel?: {
    workerPath: string;   // Absolute path to worker transform file
    poolSize: number;     // Number of worker threads to spawn
    concurrency?: number; // Max in-flight chunks (default: poolSize * 2)
  };

  // Cancellation support
  signal?: AbortSignal;

  // Progress reporting
  onProgress?: (info: {
    totalBytes: number;
    chunksProcessed: number;
    elapsedMs: number;
  }) => void;
}
```

`transform` and `parallel` are mutually exclusive — use one or the other.

### `IngestionResult`

```typescript
interface IngestionResult {
  totalBytes: number;
  duration: number;
  status: 'success' | 'aborted' | 'failed';
  error?: Error;
  sinkAbortError?: Error;
  chunksProcessed: number;
  throughputBytesPerSec: number;
}
```

| Field | Description |
|-------|-------------|
| `totalBytes` | Total bytes successfully written to the sink |
| `duration` | Processing time in milliseconds |
| `status` | `"success"` — completed normally, `"aborted"` — cancelled via AbortSignal, `"failed"` — error occurred |
| `error` | The error if status is `"aborted"` or `"failed"` |
| `sinkAbortError` | If the sink's `abort()` also threw, that error is captured here |
| `chunksProcessed` | Number of chunks successfully written |
| `throughputBytesPerSec` | Average throughput in bytes per second |

### `WorkerTransformFunction`

```typescript
type WorkerTransformFunction = (chunk: Buffer) => Promise<Buffer | null>;
```

The type signature for worker transform functions. Return `null` to skip a chunk.

### `createFsSink(path)`

Creates an `IngestionSink` that writes to a file. Handles backpressure via the `drain` event.

## Benchmarks

Run with `npm run bench` after cloning the repo.

### Serial vs Parallel Throughput

10 chunks, 200ms CPU work per chunk:

| Mode | Pool Size | Time | Speedup |
|------|-----------|------|---------|
| Serial | — | 2003ms | 1.0x |
| Parallel | 4 | 808ms | 2.5x |
| Parallel | 10 | 726ms | 2.8x |

### Memory Stability

Constant RSS regardless of dataset size:

| Dataset | Library RSS | Naive RSS (estimated) |
|---------|-------------|----------------------|
| 10 MB | 74 MB | ~20 MB |
| 50 MB | 124 MB | ~100 MB |
| 100 MB | 165 MB | ~200 MB |
| 200 MB | 265 MB | ~400 MB |
| 1 GB+ | **flat** | **crashes** |

Naive approach buffers the entire payload in memory. This library streams chunk-by-chunk with backpressure.

### Event Loop Responsiveness

10 chunks, 100ms CPU work per chunk:

| Mode | Duration | Max Event Loop Delay | Blocks Server? |
|------|----------|---------------------|----------------|
| Main-thread transform | 1002ms | 101ms | Yes |
| Worker-thread transform | 786ms | 24ms | No |

Worker threads keep your server responsive under heavy processing load.

## Error Handling

### Sink Errors

If `sink.write()` throws, ingestion stops immediately, `sink.abort()` is called, and the result contains the error:

```typescript
const result = await ingestStream(source, sink);
if (result.status === 'failed') {
  console.error('Ingestion failed:', result.error?.message);
  if (result.sinkAbortError) {
    console.error('Additionally, sink.abort() failed:', result.sinkAbortError.message);
  }
}
```

### Transform Errors

In serial mode, throwing from `transform` stops ingestion. In parallel mode, a worker error terminates the entire pipeline.

```typescript
await ingestStream(source, sink, {
  transform: async (chunk) => {
    if (!isValid(chunk)) throw new Error('Invalid chunk');
    return process(chunk);
  }
});
```

## Troubleshooting

### Worker Not Found

```
Error: Cannot find module './my-worker.js'
```

Use absolute paths for worker scripts:

```typescript
workerPath: path.resolve(__dirname, 'my-worker.js')
```

### High Memory Usage

Reduce the concurrency limit:

```typescript
parallel: {
  workerPath: './worker.js',
  poolSize: 4,
  concurrency: 2 // fewer in-flight chunks
}
```

Or switch to serial processing for simpler workloads.

### Chunks Out of Order

The library guarantees order preservation. If data appears out of order, check your sink implementation — make sure `write()` fully completes before resolving.

## Requirements

- Node.js >= 18
- Zero runtime dependencies

## License

ISC — see [LICENSE](./LICENSE)
