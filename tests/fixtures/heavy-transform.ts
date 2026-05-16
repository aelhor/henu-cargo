import { WorkerTransformFunction } from "../../src/transforms/types.js";

const heavyTransform: WorkerTransformFunction = async (chunk) => {
    const start = Date.now();
    while (Date.now() - start < 200) {}
    return chunk;
};

export default heavyTransform;
