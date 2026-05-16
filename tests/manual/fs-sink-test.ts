// tests/manual/fs-sink-test.ts
import { Readable } from "node:stream";
import { ingestStream } from "../../src/core/ingest-stream";
import { createFsSink } from "../../src/sinks/fs-sink";

async function main() {
  const source = Readable.from([
    Buffer.from("Hello "),
    Buffer.from("world"),
    Buffer.from("!\n")
  ]);

  const sink = createFsSink("./output.dat");

  await ingestStream(source, sink);

  console.log("Done");
}

main().catch(console.error);
