import js from "@eslint/js";
import prettier from "eslint-config-prettier";
import globals from "globals";
import vue from "eslint-plugin-vue";
import vueA11y from "eslint-plugin-vuejs-accessibility";
import tseslint from "typescript-eslint";

// One lint configuration for the whole repository, organized by file kind.
// Formatting is prettier's job — eslint-config-prettier comes last so the
// two never fight.
export default tseslint.config(
  {
    ignores: [
      "**/dist/",
      "**/coverage/",
      "**/target/",
      "node_modules/",
      "pnpm-lock.yaml",
      // The boundary canaries in tools/fixtures/ import across layers on
      // purpose; Archkeep judges them, not eslint.
      "tools/fixtures/",
    ],
  },

  js.configs.recommended,

  {
    // Repository scripts and tools are plain Node modules; stdout is their
    // interface, so no-console stays off here.
    files: ["**/*.{js,mjs,cjs}"],
    languageOptions: {
      globals: { ...globals.node },
    },
    rules: {
      "no-console": "off",
    },
  },

  {
    files: ["**/*.{ts,vue}"],
    extends: [
      tseslint.configs.strictTypeChecked,
      tseslint.configs.stylisticTypeChecked,
    ],
    languageOptions: {
      parserOptions: {
        projectService: true,
        extraFileExtensions: [".vue"],
      },
    },
    rules: {
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      "@typescript-eslint/consistent-type-imports": [
        "error",
        { prefer: "type-imports", fixStyle: "inline-type-imports" },
      ],
      "@typescript-eslint/restrict-template-expressions": [
        "error",
        { allowNumber: true, allowBoolean: true },
      ],
    },
  },

  {
    files: ["**/*.vue"],
    extends: [
      vue.configs["flat/recommended"],
      vueA11y.configs["flat/recommended"],
    ],
    languageOptions: {
      parserOptions: {
        // vue-eslint-parser owns the file and must be told which parser
        // handles the <script> block, otherwise the type-aware rules above
        // run without type information.
        parser: tseslint.parser,
        projectService: true,
        extraFileExtensions: [".vue"],
      },
    },
    rules: {
      // `App` is the conventional root-component name; everything else must
      // be multi-word.
      "vue/multi-word-component-names": ["error", { ignores: ["App"] }],
      // Script setup only — matches Loom and ADR 0004.
      "vue/component-api-style": ["error", ["script-setup"]],
    },
  },

  {
    // Entry files import an SFC default export. The tsserver that powers
    // typed linting types `.vue` modules only through vue-tsc, so the
    // imported component is an error type here and the no-unsafe rules
    // misfire on correct code. vue-tsc (the `web:typecheck` task) is the
    // authoritative checker for SFC boundaries; this override stays scoped
    // to entry files and touches nothing else.
    files: ["**/src/main.ts"],
    rules: {
      "@typescript-eslint/no-unsafe-argument": "off",
    },
  },

  {
    files: ["**/*.test.ts", "**/*.spec.ts", "**/test-setup.ts"],
    rules: {
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
    },
  },

  {
    rules: {
      eqeqeq: ["error", "smart"],
      "no-var": "error",
      "prefer-const": "error",
    },
  },

  prettier,
);
