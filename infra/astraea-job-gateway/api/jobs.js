import getRawBody from "raw-body";
import { Receiver } from "@upstash/qstash";
import { Redis } from "@upstash/redis";

const prefix = process.env.ASTRAEA_REDIS_PREFIX || "astraea:v1";
const allowedNodes = new Set(
  (process.env.ASTRAEA_DURABLE_NODES || "auto,papeleria,latitude,maquina-01,maquina-02,maquina-03")
    .split(",").map(x => x.trim()).filter(Boolean)
);
const allowedTools = new Set(
  (process.env.ASTRAEA_DURABLE_TOOLS || "terminal_exec")
    .split(",").map(x => x.trim()).filter(Boolean)
);

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const receiver = new Receiver({
  currentSigningKey: process.env.QSTASH_CURRENT_SIGNING_KEY,
  nextSigningKey: process.env.QSTASH_NEXT_SIGNING_KEY,
});

function validId(value, max = 128) {
  return typeof value === "string" &&
    value.length > 0 &&
    value.length <= max &&
    /^[A-Za-z0-9_.:-]+$/.test(value);
}

export default async function handler(req, res) {
  if (req.method === "GET") {
    return res.status(200).json({
      ok: true,
      service: "astraea-job-gateway",
      version: "0.2.0"
    });
  }

  if (req.method !== "POST") {
    res.setHeader("Allow", "GET, POST");
    return res.status(405).json({ ok: false, error: "method_not_allowed" });
  }

  const raw = await getRawBody(req);
  const rawText = raw.toString("utf8");
  const signature = req.headers["upstash-signature"];

  if (!signature || typeof signature !== "string") {
    return res.status(401).json({ ok: false, error: "missing_signature" });
  }

  try {
    await receiver.verify({
      signature,
      body: rawText,
      upstashRegion: typeof req.headers["upstash-region"] === "string"
        ? req.headers["upstash-region"]
        : undefined,
      clockTolerance: 5
    });
  } catch (error) {
    console.error("qstash_verify_failed", {
      message: error instanceof Error ? error.message : String(error),
      name: error instanceof Error ? error.name : "unknown",
      region: typeof req.headers["upstash-region"] === "string" ? req.headers["upstash-region"] : null,
      host: req.headers.host || null,
      body_length: rawText.length
    });
    return res.status(401).json({ ok: false, error: "invalid_signature" });
  }

  let job;
  try {
    job = JSON.parse(rawText);
  } catch {
    return res.status(400).json({ ok: false, error: "invalid_json" });
  }

  const jobId = String(job.job_id || "");
  const traceId = String(job.trace_id || jobId);
  const nodeId = String(job.node_id || "auto");
  const tool = String(job.tool || "");
  const priorityRaw = String(job.priority || "normal").toLowerCase();
  const priority = ["high", "normal", "low"].includes(priorityRaw) ? priorityRaw : "normal";
  const role = job.role == null ? null : String(job.role);
  const capabilityRequired = String(job.capability_required || tool || "terminal_exec");
  const deadline = job.deadline == null ? null : Number(job.deadline);
  const jobType = String(job.job_type || "tool");
  const createdBy = String(job.created_by || "qstash").slice(0, 128);
  const args = job.arguments && typeof job.arguments === "object" && !Array.isArray(job.arguments)
    ? job.arguments : {};

  if (
    !validId(jobId) ||
    !validId(traceId) ||
    !validId(nodeId) ||
    !validId(tool) ||
    !validId(capabilityRequired) ||
    !validId(jobType)
  ) {
    return res.status(400).json({ ok: false, error: "invalid_job_identity" });
  }

  if (!allowedNodes.has(nodeId)) {
    return res.status(403).json({ ok: false, error: "node_not_allowed" });
  }

  if (!allowedTools.has(tool)) {
    return res.status(403).json({ ok: false, error: "tool_not_allowed" });
  }

  const submitKey = `${prefix}:job-submit:${jobId}`;
  const accepted = await redis.set(submitKey, "1", { nx: true, ex: 86400 });
  if (!accepted) {
    return res.status(200).json({ ok: true, duplicate: true, job_id: jobId, trace_id: traceId });
  }

  const now = Date.now();
  const record = {
    schema_version: 2,
    job_type: jobType,
    priority,
    job_id: jobId,
    trace_id: traceId,
    node_id: nodeId,
    role,
    capability_required: capabilityRequired,
    created_by: createdBy,
    deadline: Number.isFinite(deadline) ? deadline : null,
    tool,
    arguments: args,
    lock_key: typeof job.lock_key === "string" ? job.lock_key.slice(0, 256) : null,
    lock_ttl_ms: Math.max(1000, Math.min(Number(job.lock_ttl_ms || 30000), 300000)),
    status: "queued",
    created_at: now,
    updated_at: now,
  };

  try {
    const encoded = JSON.stringify(record);
    await redis.set(`${prefix}:job:${jobId}`, encoded, { ex: 86400 });
    await redis.rpush(`${prefix}:queue:durable:${priority}`, encoded);
    return res.status(202).json({
      ok: true,
      queued: true,
      priority,
      node_id: nodeId,
      job_id: jobId,
      trace_id: traceId
    });
  } catch {
    await redis.del(submitKey);
    return res.status(503).json({ ok: false, error: "queue_unavailable" });
  }
}

export const config = {
  api: {
    bodyParser: false,
  },
};
