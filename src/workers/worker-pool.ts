//  manages multiple workers. It's the bridge between ingestStream.ts and worker-entry.ts.
// it's job is : 
// 1. Spawns N workers at startup
// 2. Initializes each worker with the transform file path
// 3. Dispatches chunks to available workers
// 4. Collects results (with sequence IDs)
// 5. Terminates all workers when done or on error

import path from "path"
import fs from "node:fs"
import { Worker } from "worker_threads"
import { messageTypes } from "./message-types"

// You have N workers and M chunks (M >> N). 
// You need to reuse workers — when Worker A finishes a task, give it the next chunk.
interface PendingTask {
    resolve: (value: any) => void
    reject: (error: Error) => void
}
export class WorkerPool {
    private workers: Worker[] = []
    private available: Worker[] = []
    private taskQueue: { chunk: Buffer; sequenceId: number }[] = []
    private pending: Map<number, PendingTask> = new Map() // Map of sequenceId to PendingTask
    onResult?: (result: { chunk: Buffer | null; sequenceId: number }) => void
    onError?: (error: Error, sequenceId: number) => void


    private constructor(poolSize: number, transformPath: string) {
    }

    /**
     * Creates a new worker pool and initializes N workers (mark a worker as ready when it sends a ready message)
     * @param poolSize Number of workers to create
     * @param transformPath Path to the transform file
     * @returns A new WorkerPool instance
     */
    static async create(poolSize: number, transformPath: string): Promise<WorkerPool> {
        const pool = new WorkerPool(poolSize, transformPath);

        const workerEntryPath = path.resolve(__dirname, 'worker-entry.js');

        if (!fs.existsSync(workerEntryPath)) {
            throw new Error(
                `Worker entry file not found at ${workerEntryPath}. ` +
                'Ensure the package was built correctly (npm run build).'
            );
        }

        for (let i = 0; i < poolSize; i++) {
            const worker = new Worker(workerEntryPath);
            worker.postMessage({ type: messageTypes.init, transformPath });

            // wait for the worker to send an ack message (confirm its ready)
            await new Promise((resolve, reject) => {
                worker.on('message', function initHandler(message) {
                    if (message.type === messageTypes.ready) {
                        worker.removeListener('message', initHandler);
                        resolve(true);
                    }
                    if (message.type === messageTypes.error) {
                        worker.removeListener('message', initHandler);
                        reject(new Error(message.error));
                    }
                })
            });
            // NOW safe — worker has loaded the transform
            worker.on('message', (msg) => pool.handelWorkerMessage(worker, msg));
            pool.workers.push(worker);
            pool.available.push(worker);
        }

        return pool;
    }

    /**
     * Dispatch a chunk to an available worker
     * @param chunk The chunk to process
     * @param sequenceId The sequence ID for tracking
     * @returns A promise that resolves with the processed chunk and sequence ID
    */
    dispatch(chunk: Buffer, sequenceId: number): Promise<{ chunk: Buffer | null, sequenceId: number }> {
        const promise = new Promise<{ chunk: Buffer | null, sequenceId: number }>((resolve, reject) => {
            this.pending.set(sequenceId, { resolve, reject });
        });

        // Swallow unhandled rejections. In fire-and-forget mode (parallel
        // orchestrator) the promise is never awaited — errors are reported
        // through the onError callback instead.
        promise.catch(() => {});

        const availableWorker = this.available.shift();
        if (availableWorker) {
            availableWorker.postMessage({ type: messageTypes.task, chunk, sequenceId });
        } else {
            this.taskQueue.push({ chunk, sequenceId });
        }

        return promise;
    }

    /**
     * Process the task queue by dispatching tasks to available workers
     */
    private processQueue() {
        while (this.taskQueue.length && this.available.length) {
            const task = this.taskQueue.shift();
            const worker = this.available.shift();
            if (task && worker) {
                worker.postMessage({ type: messageTypes.task, chunk: task.chunk, sequenceId: task.sequenceId });
            }
        }
    }


    private handelWorkerMessage(worker: Worker, message: any) {
        const sequenceId = message.sequenceId;
        const pending = this.pending.get(sequenceId);
        if (!pending) return;

        if (message.type === messageTypes.result) {
            // Worker completed successfully — resolve the pending dispatch promise
            // and forward the result through the onResult callback so the
            // parallel orchestrator can feed it into the Sequencer without
            // awaiting the dispatch.
            pending.resolve({ chunk: message.chunk, sequenceId: message.sequenceId });
            this.onResult?.({ chunk: message.chunk, sequenceId: message.sequenceId });
        } else {
            // Worker transform threw — reject the pending promise and notify
            // the parallel orchestrator via onError so it can kill the pipeline.
            const error = new Error(message.error);
            pending.reject(error);
            this.onError?.(error, message.sequenceId);
        }

        this.pending.delete(sequenceId);
        // Return this worker to the available pool and pump the queue so any
        // queued chunks can be dispatched immediately.
        this.available.push(worker);
        this.processQueue();
    }
    
    /**
     * Terminate all workers
     */
    terminate() {
        for (const worker of this.workers) {
            worker.terminate();
        }
    }
}
