import { run as serialVsParallel } from "./serial-vs-parallel";
import { run as memoryStability } from "./memory-stability";
import { run as backpressure } from "./backpressure";
import { run as eventLoopDelay } from "./event-loop-delay";
import { run as yieldOverhead } from "./yield-overhead";

async function main() {
    console.log("\n╔══════════════════════════════════════════════════╗");
    console.log("║     stream-ingestion — Benchmark Suite           ║");
    console.log("╚══════════════════════════════════════════════════╝");

    await serialVsParallel();
    await memoryStability();
    await backpressure();
    await eventLoopDelay();
    await yieldOverhead();

    console.log("━".repeat(50));
    console.log("All benchmarks complete.\n");
}

main().catch((err) => {
    console.error("Benchmark failed:", err);
    process.exit(1);
});
