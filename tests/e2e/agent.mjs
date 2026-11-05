// A simulated agent desktop (parity-cc-routing M1 e2e): a dev token for acme/agent-a1 signed with
// the gateway's local-dev key (never printed), agent-ws.v1 frames to the real gateway: auth, ready,
// accept the offer, wrap-up done; it exits 0 once it is READY again after the wrap-up.
//   node agent.mjs jwks <private jwk> <out jwks.json>
//   node agent.mjs run  <private jwk> <gateway port> <timeline out>
import crypto from "node:crypto";
import fs from "node:fs";
const [mode, jwkPath, arg, out] = process.argv.slice(2);
const jwk = JSON.parse(fs.readFileSync(jwkPath, "utf8"));
const priv = crypto.createPrivateKey({ key: jwk, format: "jwk" });
const kid = jwk.kid;
if (mode === "jwks") {
  const pub = crypto.createPublicKey(priv).export({ format: "jwk" });
  if (kid) pub.kid = kid;
  pub.use = "sig";
  pub.alg = "ES256";
  fs.writeFileSync(arg, JSON.stringify({ keys: [pub] }));
  process.exit(0);
}
const b64 = (b) => Buffer.from(b).toString("base64url");
const head = { alg: "ES256", typ: "JWT", ...(kid ? { kid } : {}) };
const claims = { iss: "parity-gateway-dev", sub: "agent-a1", tenant_id: "acme", agent_id: "agent-a1",
  roles: ["agent"], sid: crypto.randomUUID(), exp: Math.floor(Date.now() / 1000) + 1800 };
const data = `${b64(JSON.stringify(head))}.${b64(JSON.stringify(claims))}`;
const sig = crypto.sign("sha256", Buffer.from(data), { key: priv, dsaEncoding: "ieee-p1363" });
const token = `${data}.${b64(sig)}`;
const t0 = Date.now();
const lines = [];
const note = (what, o = {}) => { const l = JSON.stringify({ t_ms: Date.now() - t0, what, ...o }); lines.push(l); console.log(l); };
// acme.localhost resolves to ::1 only here and the gateway binds 127.0.0.1: connect by address and
// name the tenant host the way the gateway reads it (host-header: x-forwarded-host, as Traefik sets it).
const ws = new WebSocket(`ws://127.0.0.1:${arg}/agents/v1/ws`, { headers: { "x-forwarded-host": `acme.localhost:${arg}`, origin: `http://acme.localhost:${arg}` } });
let readyAfterWrap = false, wrapped = false, accepted = false;
const send = (f) => { ws.send(JSON.stringify({ v: 1, ...f })); note("sent", { type: f.type }); };
const done = (code) => { fs.writeFileSync(out, lines.join("\n") + "\n"); process.exit(code); };
setTimeout(() => { note("timeout"); done(1); }, 240_000);
ws.onopen = () => { note("open"); ws.send(JSON.stringify({ v: 1, type: "auth", token, client_version: "cr-e2e" })); note("sent", { type: "auth" }); };
ws.onclose = (e) => { note("closed", { code: e.code }); done(readyAfterWrap ? 0 : 1); };
ws.onerror = () => note("error");
ws.onmessage = (ev) => {
  const f = JSON.parse(ev.data);
  const keep = { type: f.type };
  for (const k of ["state", "reason_code", "offer_id", "interaction_id", "queue_id", "reservation_id", "ok", "code", "ends_at_ms"]) if (f[k] !== undefined) keep[k] = f[k];
  if (f.agent) keep.state = f.agent.state;
  note("recv", keep);
  if (f.type === "snapshot") send({ type: "set_state", id: crypto.randomUUID(), state: "ready" });
  if (f.type === "offer" && !accepted) { accepted = true; send({ type: "accept", id: crypto.randomUUID(), offer_id: f.offer_id }); }
  if (f.type === "wrap_up" && !wrapped) {
    wrapped = true;
    setTimeout(() => send({ type: "wrap_up_done", id: crypto.randomUUID(), reservation_id: f.reservation_id, disposition_code: "sale" }), 2000);
  }
  if (f.type === "agent_state" && wrapped && (f.state ?? f.agent?.state) === "ready") { readyAfterWrap = true; note("ready again after wrap-up"); setTimeout(() => ws.close(), 500); }
};
