// Fixture for crates/celld/tests/workflow_rollback.rs. Every forward step
// and rollback handler appends to a per-instance Log object, which outlives
// node crashes, so the test can assert order and at-least-once counts.
import { DurableObject, WorkflowEntrypoint } from "cloudflare:workers";
import { NonRetryableError } from "cloudflare:workflows";

export class Log extends DurableObject {
  async add(entry) {
    const log = (await this.ctx.storage.get("log")) ?? [];
    log.push(entry);
    await this.ctx.storage.put("log", log);
    return log.filter((e) => e.step === entry.step).length;
  }
  async read() {
    return (await this.ctx.storage.get("log")) ?? [];
  }
  async allow() {
    await this.ctx.storage.put("allow", true);
  }
  async allowed() {
    return (await this.ctx.storage.get("allow")) ?? false;
  }
}

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
// The code-drift test rewrites this line between node restarts: "handler"
// registers the rollback, "none" drops it, "config" changes its config.
const DRIFT = "handler";
const once = { retries: { limit: 0, delay: 0 } };

export class Saga extends WorkflowEntrypoint {
  async run(event, step) {
    const log = this.env.LOG.getByName(event.instanceId);
    const forward = (label, value) => async () => {
      await log.add({ step: "fwd:" + label });
      return value;
    };
    const undo = (label, options = {}) => ({
      rollback: async ({ ctx, error, output }) => {
        const seen = await log.add({
          step: label,
          output: output ?? null,
          error: `${error.name}:${error.message}`,
          ctx: [ctx.step.name, ctx.step.count, ctx.attempt],
        });
        if (options.failUntilAllowed && !(await log.allowed())) {
          throw new Error("undo failed for " + label);
        }
        if (options.failFirst && seen === 1) throw new Error("transient " + label);
        if (options.slow) await wait(options.slow);
      },
      ...(options.config ? { rollbackConfig: options.config } : {}),
    });
    switch (event.payload.scenario) {
      case "chain":
        await step.do("a", forward("a", "out-a"), undo("a"));
        await step.do("b", once, forward("b", "out-b"), undo("b", { config: event.payload.bConfig }));
        await step.do("plain", forward("plain", "p"));
        await step.do("boom", once, async () => { throw new NonRetryableError("boom"); });
        return "unreachable";
      case "repeat":
        for (let i = 0; i < 3; i++) await step.do("same", forward("same", "same-" + i), undo("same"));
        throw new NonRetryableError("repeat");
      case "parallel": {
        let release;
        const gate = new Promise((resolve) => (release = resolve));
        const first = step.do("first", async () => { await gate; await wait(100); return "out-1"; }, undo("first"));
        await wait(50);
        const second = step.do("second", async () => { release(); return "out-2"; }, undo("second"));
        await Promise.all([first, second]);
        throw new NonRetryableError("parallel");
      }
      case "caught":
        await step.do("a", forward("a", "out-a"), undo("a"));
        try {
          await step.do("bad", once, async () => { throw new Error("bad"); }, undo("bad"));
        } catch {}
        return "ok";
      case "failed-step":
        await step.do("a", forward("a", "out-a"), undo("a"));
        await step.do("bad", once, async () => { throw new Error("forward bad"); }, undo("bad"));
        return "unreachable";
      case "transient":
        await step.do("a", forward("a", "out-a"), undo("a", {
          failFirst: true,
          config: { retries: { limit: 3, delay: "1 second", backoff: "linear" } },
        }));
        throw new NonRetryableError("transient");
      case "halt":
        await step.do("a", forward("a", "out-a"), undo("a"));
        await step.do("b", forward("b", "out-b"), undo("b", {
          failUntilAllowed: true,
          config: { retries: { limit: 1, delay: "1 second" } },
        }));
        throw new NonRetryableError("halt");
      case "sleeper":
        await step.do("a", forward("a", "out-a"), undo("a"));
        await step.sleep("forever", "1 hour");
        return "unreachable";
      case "in-flight":
        await step.do("a", forward("a", "out-a"), undo("a"));
        await step.do("slow", { retries: { limit: 0, delay: 0 }, timeout: "20 seconds" },
          async () => { await wait(2000); return "out-slow"; }, undo("slow"));
        return "unreachable";
      case "sibling":
        await Promise.all([
          step.do("reserve", async () => { await wait(1500); return "out-reserve"; }, undo("reserve")),
          step.do("charge", once, async () => { throw new NonRetryableError("charge"); }),
        ]);
        return "unreachable";
      case "branches":
        await Promise.all([
          (async () => {
            await step.do("a1", async () => "out-a1");
            await step.do("a2", async () => "out-a2", undo("a2"));
          })(),
          step.do("b", once, async () => { await wait(500); throw new NonRetryableError("b"); }),
        ]);
        return "unreachable";
      case "slow-undo":
        await step.do("a", forward("a", "out-a"), undo("a"));
        await step.do("b", forward("b", "out-b"), undo("b", { slow: 3000 }));
        throw new NonRetryableError("slow-undo");
      case "restart-from":
        await step.do("a", forward("a", "out-a"), undo("a"));
        await step.do("b", once, async () => { throw new NonRetryableError("b"); });
        return "unreachable";
      case "drift": {
        const options = DRIFT === "none"
          ? []
          : [undo("a", DRIFT === "config" ? { config: { timeout: "5 seconds" } } : {})];
        await step.do("a", forward("a", "out-a"), ...options);
        await step.sleep("forever", "1 hour");
        return "unreachable";
      }
      case "bad-options":
        await step.do("a", async () => 1, { rollback: 5 });
        return "unreachable";
      case "bad-config":
        await step.do("a", async () => 1, {
          rollback: async () => {},
          rollbackConfig: { sensitive: "output" },
        });
        return "unreachable";
    }
    throw new Error("unknown scenario " + event.payload.scenario);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = url.searchParams.get("id");
    const instance = () => env.WF.get(id);
    try {
      switch (url.pathname) {
        case "/create": {
          const params = JSON.parse(url.searchParams.get("params"));
          await env.WF.create({ id, params });
          return Response.json({ ok: true });
        }
        case "/status":
          return Response.json(await (await instance()).status());
        case "/log":
          return Response.json(await env.LOG.getByName(id).read());
        case "/allow":
          await env.LOG.getByName(id).allow();
          return Response.json({ ok: true });
        case "/terminate": {
          const options = url.searchParams.get("options");
          await (await instance()).terminate(options === null ? undefined : JSON.parse(options));
          return Response.json({ ok: true });
        }
        case "/retry-rollback":
          await (await instance()).retryRollback();
          return Response.json({ ok: true });
        case "/pause":
          await (await instance()).pause();
          return Response.json({ ok: true });
        case "/restart": {
          const from = url.searchParams.get("from");
          await (await instance()).restart(from === null ? undefined : { from: { name: from } });
          return Response.json({ ok: true });
        }
        case "/delete":
          await (await instance()).delete();
          return Response.json({ ok: true });
        case "/event":
          await (await instance()).sendEvent({ type: "x" });
          return Response.json({ ok: true });
      }
    } catch (error) {
      return Response.json({ error: String(error?.message ?? error) });
    }
    return new Response("not found", { status: 404 });
  },
};
