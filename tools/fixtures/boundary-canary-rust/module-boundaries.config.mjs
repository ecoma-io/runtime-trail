// Deliberately-wrong-by-construction policy for the Rust boundary canary.
// It states runtime-trail's real rule for `layer-storage-driver` (drivers may
// depend on layer-storage and layer-model only) while the `driver` crate
// below carries a Cargo path dependency on `core`, tagged
// `layer-correlation` — the forbidden "driver reaches the correlation engine"
// edge of docs/architecture/boundaries.md. The check over this tree must exit
// 1 with exactly `onlyTagsConstraintViolation`, which is what
// tools/check-arch-canary.mjs asserts. Never fix the violation: the fixture
// exists to fail.
export const depConstraints = [
  {
    sourceTag: "layer-storage-driver",
    onlyDependOnLibsWithTags: ["layer-storage", "layer-model"],
  },
  {
    sourceTag: "layer-correlation",
    onlyDependOnLibsWithTags: ["layer-correlation"],
  },
];

export const moduleBoundaryOptions = {
  allow: [],
  buildTargets: ["build"],
  enforceBuildableLibDependency: false,
  allowCircularSelfDependency: false,
  checkDynamicDependenciesExceptions: [],
  ignoredCircularDependencies: [],
  banTransitiveDependencies: false,
  checkNestedExternalImports: false,
};

export const boundarySuppressions = [];
