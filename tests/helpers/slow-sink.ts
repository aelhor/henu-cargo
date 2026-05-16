import { IngestionSink } from "../../src/sinks/Ingestion-sink";

export function createSlowSink(): IngestionSink {


    const pending: Buffer[] = [];
    const inFlight = new Set<Promise<void>>();

    const slowSink: IngestionSink = {
        write: async (chunk) => {
            pending.push(chunk);

            const p = (async () => {
                // Simulate slow I/O (Database, Network, etc) waits 50ms
                await new Promise((res) => setTimeout(res, 200));
            })().finally(() => {
                pending.shift();
            });

            inFlight.add(p);
            p.finally(() => inFlight.delete(p));

            await p;
        },
        finalize: async () => {
            // Wait for all queued writes to finish
            await Promise.all(Array.from(inFlight));
        },
        abort: async (_error: Error) => {
            await Promise.allSettled(Array.from(inFlight));
        }
    }
    return slowSink
}