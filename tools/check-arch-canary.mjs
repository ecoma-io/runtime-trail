#!/usr/bin/env node
// Boundary-law canary runner. `pnpm arch` proves the law holds on the real
// tree; this script proves the law can actually bite — a gate that has never
// fired on a wrong tree cannot be trusted when it stays quiet on the right
// one. Each fixture under tools/fixtures/ is its own archkeep workspace,
// deliberately violating docs/architecture/boundaries.md in one concrete
// way; the runner asserts each check (a) refuses to pass (exit 1, verdict
// "fail"), (b) names the exact violation the boundary doc forbids, and
// (c) reached a verdict with complete coverage — a fixture that could not be
// analyzed would be a vacuous pass-in-reverse, not evidence.
//
// Run via `pnpm arch:canary`. CI runs it beside `pnpm arch`; both must stay
// green for the architecture gate to mean anything.

import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

const fixtures = [
  {
    name: "boundary-canary-rust",
    expect: {
      messageId: "onlyTagsConstraintViolation",
      sourceProject: "canary-driver",
      targetProject: "canary-core",
    },
  },
  {
    name: "boundary-canary-ts",
    expect: {
      messageId: "noRelativeOrAbsoluteImportsAcrossLibraries",
      sourceProject: "canary-view",
      targetProject: "canary-storage",
    },
  },
];

/** @returns {{ ok: boolean, report: object | null, detail: string }} */
function runArchkeep(cwd, expectedExit) {
  const run = spawnSync(
    "pnpm",
    ["exec", "archkeep", "check", "--format", "json"],
    {
      cwd,
      encoding: "utf8",
    },
  );
  if (run.error) return { ok: false, report: null, detail: String(run.error) };
  if (run.status !== expectedExit) {
    return {
      ok: false,
      report: null,
      detail: `archkeep exited ${run.status}, expected ${expectedExit}\nstdout: ${run.stdout}\nstderr: ${run.stderr}`,
    };
  }
  return { ok: true, report: JSON.parse(run.stdout), detail: "" };
}

let failures = 0;

for (const fixture of fixtures) {
  const dir = join(repoRoot, "tools", "fixtures", fixture.name);
  const { ok, report, detail } = runArchkeep(dir, 1);
  if (!ok) {
    console.error(`✗ ${fixture.name}: ${detail}`);
    failures += 1;
    continue;
  }
  const violations = report.result.violations;
  const match = violations.find(
    (violation) =>
      violation.messageId === fixture.expect.messageId &&
      violation.sourceProject === fixture.expect.sourceProject &&
      violation.targetProject === fixture.expect.targetProject,
  );
  if (!match) {
    console.error(
      `✗ ${fixture.name}: expected ${fixture.expect.messageId} (${fixture.expect.sourceProject} → ${fixture.expect.targetProject}), got: ${JSON.stringify(violations)}`,
    );
    failures += 1;
    continue;
  }
  if (report.decision.verdict !== "fail") {
    console.error(
      `✗ ${fixture.name}: verdict ${report.decision.verdict}, expected "fail"`,
    );
    failures += 1;
    continue;
  }
  if (report.coverage.complete !== true) {
    console.error(
      `✗ ${fixture.name}: coverage incomplete — the failure is not proven over the whole tree: ${JSON.stringify(report.coverage)}`,
    );
    failures += 1;
    continue;
  }
  console.log(
    `✓ ${fixture.name}: ${match.messageId} at ${match.sourceFile}:${match.line} (fail verdict, complete coverage)`,
  );
}

// The other half of the proof: the gate that fires here must not fire on the
// real tree. A clean run must be a reached, complete verdict — never a
// refusal (exit 3) that would mean archkeep could not see enough to judge.
const real = runArchkeep(repoRoot, 0);
if (!real.ok) {
  console.error(`✗ real tree: ${real.detail}`);
  failures += 1;
} else if (
  real.report.decision.verdict !== "pass" ||
  real.report.coverage.complete !== true
) {
  console.error(
    `✗ real tree: verdict ${real.report.decision.verdict}, coverage.complete ${real.report.coverage.complete} — expected a complete pass`,
  );
  failures += 1;
} else {
  console.log(
    `✓ real tree: no boundary violations (${real.report.coverage.imports} imports in ${real.report.coverage.analyzedFiles} files across ${real.report.coverage.projects} projects)`,
  );
}

if (failures > 0) {
  console.error(`arch canary: ${failures} failure(s)`);
  process.exit(1);
}
console.log("arch canary: all fixtures fail as designed, real tree clean");
