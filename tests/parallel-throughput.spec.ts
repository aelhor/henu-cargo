import test from 'node:test';
import assert from 'node:assert';
import { Readable } from 'node:stream';
import path from 'node:path';
import os from 'node:os';
import { ingestStream } from '../src/core/ingest-stream';

test('Benchmark: Parallel vs Serial throughput', async () => {
    const CHUNK_COUNT = 10;
    const MS_PER_CHUNK = 200;

    const heavyTransform = async (chunk: Buffer) => {
        const start = Date.now();
        while (Date.now() - start < MS_PER_CHUNK) {}
        return chunk;
    };

    const createSink = () => ({
        write: async () => {},
        finalize: async () => {},
        abort: async () => {}
    });

    const data = Array.from({ length: CHUNK_COUNT }, (_, i) => Buffer.from(`chunk-${i}`));

    const serialSource = Readable.from(data);
    const serialSink = createSink();
    const serialRes = await ingestStream(serialSource, serialSink, { transform: heavyTransform });

    const parallelSource = Readable.from(data);
    const parallelSink = createSink();
    const transformPath = path.resolve('dist-tests/tests/fixtures/heavy-transform.js');
    const poolSize = Math.min(os.cpus().length, CHUNK_COUNT);
    const parallelRes = await ingestStream(parallelSource, parallelSink, {
        parallel: { workerPath: transformPath, poolSize }
    });

    console.log(`Serial: ${serialRes.duration}ms`);
    console.log(`Parallel: ${parallelRes.duration}ms (poolSize=${poolSize})`);

    assert.ok(parallelRes.duration < serialRes.duration,
        `Parallel (${parallelRes.duration}ms) should be faster than serial (${serialRes.duration}ms)`);

    assert.strictEqual(parallelRes.totalBytes, serialRes.totalBytes,
        'Parallel should produce same total bytes as serial');
});
