import test from 'node:test';
import assert from 'node:assert';
import { Readable } from 'node:stream';
import { ingestStream } from '../src/core/ingest-stream';

function generateChunks(sizeMB: number): Buffer[] {
    const chunkSize = 1024 * 1024;
    const chunks: Buffer[] = [];
    for (let i = 0; i < sizeMB; i++) {
        chunks.push(Buffer.alloc(chunkSize, i % 256));
    }
    return chunks;
}

test('Engine handles Sink failure gracefully', async (t) => {
    const source = Readable.from(generateChunks(10));
    let abortCalled = false;

    const chaosSink = {
        write: async (chunk: Buffer) => {
            throw new Error('DATABASE_OFFLINE');
        },
        finalize: async () => {
            assert.fail('Finalize should NOT be called on error');
        },
        abort: async (err: Error) => {
            abortCalled = true;
            assert.strictEqual(err.message, 'DATABASE_OFFLINE');
        }
    };

    const result = await ingestStream(source, chaosSink);

    assert.strictEqual(result.status, 'failed', 'The engine should return failed status');
    assert.strictEqual(result.error?.message, 'DATABASE_OFFLINE', 'The engine should include the sink error');
    assert.ok(abortCalled, 'The engine should call sink.abort()');
    assert.strictEqual(source.destroyed, true, 'The SOURCE stream must be destroyed after an error');
});


test('Should destroy source even if validation fails', async (t) => {
    const source = Readable.from(generateChunks(10));

    try {
        await ingestStream(source, null as any);
    } catch (err) {
        // expected
    }

    assert.strictEqual(source.destroyed, true, 'Source must be destroyed even if params were invalid');
});

test('Should handle source errors', async (t) => {
    const brokenSource = new Readable({
        read: function () {
            this.push('some data');
            this.destroy(new Error('SOURCE_READ_ERROR'));
        }
    })
    let abortError: Error | null = null
    const mockSink = {
        write: async () => { },
        finalize: async () => { assert.fail('Should not finalize'); },
        abort: async (err: Error) => { abortError = err; }
    };
    try {
        await ingestStream(brokenSource, mockSink);
    } catch (err: any) {
        assert.strictEqual(err.message, 'SOURCE_READ_ERROR');
    }

    assert.ok(abortError, 'Sink abort should have been called');
    assert.strictEqual((abortError as any).message, 'SOURCE_READ_ERROR');
});
