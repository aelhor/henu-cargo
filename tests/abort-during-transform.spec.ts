

import test from 'node:test';
import assert from 'node:assert';
import { Readable } from 'node:stream';
import { ingestStream } from '../src/core/ingest-stream';


test('Should NOT write to sink if aborted during transform', async () => {
    const abortController = new AbortController();
    let writeCount = 0;

    const slowTransform = async (chunk: Buffer) => {
        const start = Date.now();
        while (Date.now() - start < 200) {} // CPU Burn
        return chunk;
    };

    const sink = {
        write: async (chunk: Buffer) => {
            writeCount++; // Track if we actually wrote to the sink
        },
        finalize: async () => {},
        abort: async (err: Error) => {}
    };

    const source = Readable.from([Buffer.from('chunk1')]);

    // Send abort while the CPU is busy in the transform
    setTimeout(() => abortController.abort(), 100);

    const res = await ingestStream(source, sink, { 
        transform: slowTransform, 
        signal: abortController.signal 
    });

    assert.strictEqual(res.status, 'aborted');
    // WITHOUT yielding, writeCount will be 1 (The engine didn't check the signal before writing)
    // WITH yielding, writeCount should be 0 (The engine checked the signal after the transform)
    assert.strictEqual(writeCount, 0, 'Should have aborted before writing to sink');
});


test('Should NOT include bytes when transforms return null', async () => {
    const abortController = new AbortController();
    let writeCount = 0;

    const slowTransform = async (chunk: Buffer) => {
        if (chunk.toString() === 'filter-me') {
            return null;
        }
        return chunk;
    };

    const sink = {
        write: async (chunk: Buffer) => {
            writeCount++; // Track if we actually wrote to the sink
        },
        finalize: async () => {},
        abort: async (err: Error) => {}
    };

    const source = Readable.from([Buffer.from('filter-me'), Buffer.from('keep-me')]);

    // Send abort while the CPU is busy in the transform
    // setTimeout(() => abortController.abort(), 100);

    const res = await ingestStream(source, sink, { 
        transform: slowTransform, 
        signal: abortController.signal 
    });

    assert.strictEqual(writeCount, 1, 'Should have filtered out some chunks');
});


test('Should handle transform errors', async () => {
    const abortController = new AbortController();
    let writeCount = 0;

    const failingTransform = async (chunk: Buffer) => {
        throw new Error("Transform Failed");
    };

    const sink = {
        write: async (chunk: Buffer) => {
            writeCount++; // Track if we actually wrote to the sink
        },
        finalize: async () => {},
        abort: async (err: Error) => {}
    };

    const source = Readable.from([Buffer.from('test')]);

    const res = await ingestStream(source, sink, { 
        transform: failingTransform, 
        signal: abortController.signal 
    });

    assert.strictEqual(res.status, 'failed');
});
