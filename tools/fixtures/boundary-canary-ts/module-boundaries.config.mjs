// Deliberately-wrong-by-construction policy for the TypeScript boundary
// canary. It states runtime-trail's real rule for `layer-view` (the UI may
// import no workspace project — it reaches the core over HTTP only) while
// `view/src/index.ts` below imports `storage` through a cross-project
// relative path, the exact compile-time door around the Investigation API
// docs/architecture/boundaries.md forbids. The check over this tree must
// exit 1 with exactly `noRelativeOrAbsoluteImportsAcrossLibraries`, which is
// what tools/check-arch-canary.mjs asserts. Never fix the violation: the
// fixture exists to fail.
export const depConstraints = [
  { sourceTag: "layer-view", onlyDependOnLibsWithTags: ["layer-view"] },
  { sourceTag: "layer-storage", onlyDependOnLibsWithTags: ["layer-storage"] },
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
