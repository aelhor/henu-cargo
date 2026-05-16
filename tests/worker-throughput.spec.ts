import test from 'node:test';
import assert from 'node:assert';
import { Readable, Transform } from 'node:stream';
import { ingestStream } from '../src/core/ingest-stream';

test('Benchmark: Serial Throughput (The "Before" Picture)', async () => {

    const CHUNK_COUNT = 10;
    const MS_PER_CHUNK = 200; // 0.2 seconds of "math" per chunk

    const heavyTransform = async (chunk: Buffer) => {
        const start = Date.now();
        // Simulate heavy CPU work (e.g., encryption/hashing)
        while (Date.now() - start < MS_PER_CHUNK) { }
        return chunk;
    };

    const sink = {
        write: async () => { }, // Fast I/O
        finalize: async () => { },
        abort: async () => { }
    };
    // Create 10 chunks
    const data = Array.from({ length: CHUNK_COUNT }, (_, i) => Buffer.from(`chunk-${i}`));
    const source = Readable.from(data);
    console.time('MS5_Serial_Duration');
    const res = await ingestStream(source, sink, { transform: heavyTransform });
    console.timeEnd('MS5_Serial_Duration');

    // Logic: 10 chunks * 200ms = 2000ms minimum
    assert.ok(res.duration >= CHUNK_COUNT * MS_PER_CHUNK, `Should take at least ${CHUNK_COUNT * MS_PER_CHUNK}ms`);
});