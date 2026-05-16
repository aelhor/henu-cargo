import { Readable } from "node:stream";
import test from "node:test";
import assert from "node:assert";
import { ingestStream } from "../src/core/ingest-stream";
import { IngestionSink } from "../src/sinks/Ingestion-sink";

test("Basic ingestion writes all chunks, finalizes, and reports bytes", async () => {
  const received: Buffer[] = [];
  let finalized = false;
  let aborted = false;

  const sink: IngestionSink = {
    async write(chunk: Buffer) {
      received.push(chunk);
    },
    async finalize() {
      finalized = true;
    },
    async abort() {
      aborted = true;
    }
  };

  const source = Readable.from([
    Buffer.from("a"),
    Buffer.from("b")
  ]);

  const result = await ingestStream(source, sink);

  assert.strictEqual(received.map(b => b.toString()).join(""), "ab");
  assert.strictEqual(finalized, true);
  assert.strictEqual(aborted, false);
  assert.strictEqual(result.totalBytes, 2);
});
