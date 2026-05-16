import test from 'node:test';
import assert from 'node:assert';
import { Readable } from 'node:stream';
import path from 'node:path';
import os from 'node:os';
import { ingestStream } from '../src/core/ingest-stream';

test('Parallel processing preserves chunk order', async () => {
    const CHUNK_COUNT = 10;

    const receivedChunks: Buffer[] = [];
    
    const orderPreservingSink = {
        write: async (chunk: Buffer) => {
            // Store chunks as Buffer objects to preserve order
            receivedChunks.push(chunk);
        },
        finalize: async () => {},
        abort: async () => {}
    };

    // Create test data with sequential identifiers
    const data = Array.from({ length: CHUNK_COUNT }, (_, i) => 
        Buffer.from(`chunk-${i}`)
    );
    
    const source = Readable.from(data);
    const transformPath = path.resolve('dist-tests/tests/fixtures/heavy-transform.js');
    const poolSize = Math.min(os.cpus().length, CHUNK_COUNT);
    
    const result = await ingestStream(source, orderPreservingSink, {
        parallel: { 
            workerPath: transformPath, 
            poolSize
        }
    });

    console.log(`Processed ${CHUNK_COUNT} chunks with poolSize=${poolSize}`);
    console.log(`Total duration: ${result.duration}ms`);
    console.log(`Chunks received: ${receivedChunks.length}`);

    // Verify we received all chunks
    assert.strictEqual(receivedChunks.length, CHUNK_COUNT, 
        `Should receive exactly ${CHUNK_COUNT} chunks`);

    // Convert chunks to strings for order verification
    const receivedStrings = receivedChunks.map(chunk => {
        const chunkStr = chunk.toString();
        // Check if it's ASCII codes (comma-separated numbers) or actual string
        if (chunkStr.includes(',')) {
            // Convert ASCII codes back to string
            const asciiCodes = chunkStr.split(',').map(code => parseInt(code.trim()));
            return String.fromCharCode(...asciiCodes);
        }
        return chunkStr;
    });
    console.log('Received chunks (decoded):', receivedStrings);

    // Verify order is preserved by checking chunk numbers
    for (let i = 0; i < CHUNK_COUNT; i++) {
        const expectedContent = `chunk-${i}`;
        const actualContent = receivedStrings[i];
        assert.strictEqual(actualContent, expectedContent,
            `Chunk ${i} should be "${expectedContent}" but got "${actualContent}"`);
    }

    // Additional verification: ensure no duplicates
    const uniqueChunks = new Set(receivedStrings);
    assert.strictEqual(uniqueChunks.size, CHUNK_COUNT, 
        'All chunks should be unique');

    // Verify total bytes processed is correct
    const expectedTotalBytes = data.reduce((sum, chunk) => sum + chunk.length, 0);
    assert.strictEqual(result.totalBytes, expectedTotalBytes,
        'Total bytes should match input data');
});

test('Parallel order preservation with variable timing', async () => {
    const CHUNK_COUNT = 8;

    const receivedChunks: Buffer[] = [];
    
    const sink = {
        write: async (chunk: Buffer) => {
            receivedChunks.push(chunk);
        },
        finalize: async () => {},
        abort: async () => {}
    };

    // Create test data
    const data = Array.from({ length: CHUNK_COUNT }, (_, i) => 
        Buffer.from(`test-chunk-${i}`)
    );
    
    const source = Readable.from(data);
    const transformPath = path.resolve('dist-tests/tests/fixtures/heavy-transform.js');
    const poolSize = Math.min(4, CHUNK_COUNT); // Smaller pool to increase reordering chance
    
    const result = await ingestStream(source, sink, {
        parallel: { 
            workerPath: transformPath, 
            poolSize
        }
    });

    console.log(`Variable timing test - processed ${CHUNK_COUNT} chunks`);
    console.log(`Duration: ${result.duration}ms`);

    // Verify order preservation
    const receivedStrings = receivedChunks.map(chunk => {
        const chunkStr = chunk.toString();
        // Check if it's ASCII codes (comma-separated numbers) or actual string
        if (chunkStr.includes(',')) {
            // Convert ASCII codes back to string
            const asciiCodes = chunkStr.split(',').map(code => parseInt(code.trim()));
            return String.fromCharCode(...asciiCodes);
        }
        return chunkStr;
    });
    console.log('Received (decoded):', receivedStrings);

    for (let i = 0; i < CHUNK_COUNT; i++) {
        const expected = `test-chunk-${i}`;
        const actual = receivedStrings[i];
        assert.strictEqual(actual, expected,
            `Order preservation failed at position ${i}: expected "${expected}", got "${actual}"`);
    }
});
