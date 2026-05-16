import test from 'node:test';
import assert from 'node:assert';
import { Readable } from 'node:stream';
import path from 'node:path';
import { ingestStream } from '../src/core/ingest-stream';

test('onProgress is called during serial ingestion', async () => {
    const progressCalls: Array<{ totalBytes: number; chunksProcessed: number; elapsedMs: number }> = [];

    const chunks = Array.from({ length: 200 }, (_, i) => Buffer.from(`chunk-${i}`));
    const source = Readable.from(chunks);

    const sink = {
        write: async () => {},
        finalize: async () => {},
        abort: async () => {}
    };

    const result = await ingestStream(source, sink, {
        onProgress: (info) => {
            progressCalls.push({ ...info });
        }
    });

    assert.strictEqual(result.status, 'success');
    assert.ok(progressCalls.length >= 1, `Expected at least 1 progress call, got ${progressCalls.length}`);
    assert.strictEqual(result.chunksProcessed, 200);
    assert.ok(result.throughputBytesPerSec > 0);

    const firstCall = progressCalls[0];
    assert.ok(firstCall.totalBytes > 0, 'totalBytes should be > 0');
    assert.ok(firstCall.chunksProcessed > 0, 'chunksProcessed should be > 0');
    assert.ok(firstCall.elapsedMs >= 0, 'elapsedMs should be >= 0');

    const lastCall = progressCalls[progressCalls.length - 1];
    assert.ok(lastCall.chunksProcessed > 0, 'last progress call should have chunks > 0');
    assert.ok(lastCall.chunksProcessed <= 200, 'last progress call should not exceed total chunks');
});

test('onProgress is called during parallel ingestion', async () => {
    const progressCalls: Array<{ totalBytes: number; chunksProcessed: number; elapsedMs: number }> = [];

    const chunks = Array.from({ length: 200 }, (_, i) => Buffer.from(`chunk-${i}`));
    const source = Readable.from(chunks);
    const sink = {
        write: async () => {},
        finalize: async () => {},
        abort: async () => {}
    };

    const transformPath = path.resolve('dist-tests/tests/fixtures/heavy-transform.js');

    const result = await ingestStream(source, sink, {
        parallel: { workerPath: transformPath, poolSize: 4 },
        onProgress: (info) => {
            progressCalls.push({ ...info });
        }
    });

    assert.strictEqual(result.status, 'success');
    assert.ok(progressCalls.length >= 1, `Expected at least 1 progress call, got ${progressCalls.length}`);
    assert.strictEqual(result.chunksProcessed, 200);
});

test('ingestion works without onProgress (no crash)', async () => {
    const source = Readable.from([Buffer.from('a'), Buffer.from('b')]);
    const sink = {
        write: async () => {},
        finalize: async () => {},
        abort: async () => {}
    };

    const result = await ingestStream(source, sink);

    assert.strictEqual(result.status, 'success');
    assert.strictEqual(result.chunksProcessed, 2);
    assert.strictEqual(result.throughputBytesPerSec > 0, true);
});

test('IngestionResult has correct chunksProcessed and throughputBytesPerSec', async () => {
    const source = Readable.from([
        Buffer.alloc(1000, 'x'),
        Buffer.alloc(1000, 'y'),
        Buffer.alloc(1000, 'z')
    ]);
    const sink = {
        write: async () => {},
        finalize: async () => {},
        abort: async () => {}
    };

    const result = await ingestStream(source, sink);

    assert.strictEqual(result.status, 'success');
    assert.strictEqual(result.chunksProcessed, 3);
    assert.strictEqual(result.totalBytes, 3000);
    assert.ok(result.throughputBytesPerSec > 0, 'throughput should be > 0');
    assert.ok(result.duration >= 0, 'duration should be >= 0');
});
