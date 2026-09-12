/**
 * Conventional Commits with this repository's scopes. The scope list is the
 * module map: one scope per moon project plus the repository-level scopes.
 * When a new module lands, its scope lands here in the same commit.
 *
 * @type {import("@commitlint/types").UserConfig}
 */
export default {
  extends: ["@commitlint/config-conventional"],
  rules: {
    "scope-enum": [
      2,
      "always",
      [
        // crates/
        "telemetry-model",
        "storage",
        "storage-memory",
        "storage-sqlite",
        "query",
        "correlation",
        "investigation",
        "telemetry-ingestion",
        "mcp",
        "server",
        "bench-probes",
        // apps/
        "desktop",
        "web",
        // repository-level
        "arch",
        "bench",
        "ci",
        "deps",
        "docs",
        "release",
        "repo",
      ],
    ],
  },
};
