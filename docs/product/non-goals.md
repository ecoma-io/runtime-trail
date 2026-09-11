# Non-goals

> Authoritative home for what `runtime-trail` deliberately does **not** do.
> Vision: [vision.md](vision.md) · Commitments: [scope.md](scope.md)

Non-goals are decisions, not omissions. Each entry below rules something out so
that scope debates end here instead of in pull requests. A non-goal can be
revisited only by changing this document — with a decision record in
[../decisions/](../decisions/) — never silently inside a feature PR.

## Product identity

- **Not a production observability platform.** No multi-tenancy, no horizontal
  scaling, no high availability, no clustered deployment, no SLA posture.
  `runtime-trail` serves one developer's machine (or one CI job), not an
  organisation's fleet.
- **Not a lightweight Grafana clone.** The goal is investigation of one
  request/one incident across signals — not general-purpose dashboards,
  arbitrary chart builders, or organisation-wide overviews.
- **Not a hosted/cloud service.** There is no `runtime-trail` SaaS, no accounts,
  no telemetry leaving the machine.

## Telemetry handling

- **Not a general-purpose OpenTelemetry Collector.** `runtime-trail` receives
  OTLP for its own local investigation. It does not provide collector-style
  processor pipelines, tail-sampling chains, or exporter fan-out to third-party
  backends. Point your collector — or your app directly — at `runtime-trail`;
  it is a destination, not a relay.
- **No non-OTel ingestion protocols.** Vendor protocols (Jaeger thrift, Zipkin,
  Prometheus remote-write) are out of scope; OTLP is the single ingestion
  contract. (OTLP covers logs, traces and metrics — there is nothing a local
  workflow needs that OTLP cannot carry.)
- **No long-term or cross-session retention guarantees.** Storage is
  [bounded and local](../architecture/storage-model.md); `runtime-trail` is not
  a system of record. Export elsewhere if data must outlive a session beyond
  the embedded persistence mode's own retention.
- **No telemetry transformation/PII-scrubbing pipeline.** The model stays
  faithful to what the emitter sent.

## Platform surface

- **No production identity/access management.** No SSO, no RBAC, no multi-user
  auth. A local tool may bind to localhost and, when exposed on a network (e.g.
  Docker), relies on simple deployment-level controls — not an enterprise IAM
  story.
- **No alerting, SLO management or incident workflows.** These belong to
  production systems; locally they are the developer's judgement.
- **No plugin marketplace or third-party extension API.** The only supported
  external integration surface is [MCP](../architecture/mcp-model.md) and the
  Investigation API.
- **No mobile clients.**

## Ecosystem posture

- **No competition with the LGTM/production stack.** `runtime-trail` coexists
  with production backends; it never aspires to replace them.
- **No forking of Loom.** The UI consumes [Loom](https://github.com/ecoma-io/loom)
  as a dependency; gaps in Loom are fixed in Loom, not worked around here.

## Status of this list

Items here are out of scope for the committed product direction and for the
1.0 scope defined in [the roadmap](../roadmap/README.md). "Future direction"
items in the roadmap are the only place where a listed non-goal may be
reconsidered — anything else asking for one is a scope violation.
