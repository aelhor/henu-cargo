// code run inside each worked thread 
// 1. Loads the user's transform module once (when the worker starts)
// 2. Receives tasks from the main thread (chunk + sequence ID)
// 3. Runs the transform on the chunk
// 4. Sends the result back (or error) to the main thread

import { parentPort } from "worker_threads";
import { WorkerTransformFunction } from "../transforms/types";
import { messageTypes } from "./message-types";

let transform: WorkerTransformFunction | null = null;

if (!parentPort) throw new Error('This file must run inside a worker thread');

const port = parentPort; 

port.on('message', async (message: { type: messageTypes; transformPath?: string; chunk?: Buffer, sequenceId?: number }) => {
    if (message.type === messageTypes.init) {
        // Load the user's transform module via require(). Both the worker
        // entry and user transforms compile to CJS under NodeNext, so
        // require() avoids the CJS/ESM double-default interop issue that
        // dynamic import() would hit.
        const transformModule = require(message.transformPath!);
        if (!transformModule.default) {
            port.postMessage({ type: messageTypes.error, error: 'Transform module must export a default function', sequenceId: message.sequenceId });
            return
        }
        transform = transformModule.default as WorkerTransformFunction;
        // Send ack back to main thread so we can mark this as an avaliable worker
        port.postMessage({ type: messageTypes.ready, sequenceId: message.sequenceId });
        return
    }
    if (message.type === messageTypes.task) {
        // Run the transform on the chunk
        try {
            const result = await transform!(message.chunk!);
            port.postMessage({ type: messageTypes.result, chunk: result, sequenceId: message.sequenceId });
            return
        } catch (error) {
            port.postMessage({ type: messageTypes.error, error: (error as Error).message, sequenceId: message.sequenceId });
            return
        }

    }

});