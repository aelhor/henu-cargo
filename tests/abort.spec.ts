import test from 'node:test';
import assert from 'node:assert';
import { ingestStream } from '../src/core/ingest-stream';
import { Readable } from 'node:stream';
import { IngestionSink } from '../src/sinks/Ingestion-sink';

test('Should stop immediately when aborted mid-write', async (t) => {
    // craete a slow sink
    const slowSink: IngestionSink = {
        write: async (chunk: Buffer) => {
            // call abort signal
            abortController.abort('USER_CANCELLED');
        },
        finalize: async () => { },
        abort: async (err: Error) => { }
    }

    // ini AbortController 
    const abortController = new AbortController()

    // create an infinte source
    const infiniteSource = new Readable({
        read() {
            let hasSpace = true;
            // Keep pushing until the buffer is full
            while (hasSpace) {
                hasSpace = this.push(Buffer.from('data chunk\n'));
            }

        }
    })

    const result = await ingestStream(infiniteSource, slowSink, {
        signal: abortController.signal
    })

    assert.strictEqual(result.status, 'aborted');
    assert.strictEqual(result.error?.message, 'USER_CANCELLED');
    // check the source stream is destroyed
    assert.strictEqual(infiniteSource.destroyed, true);

})