<script setup lang="ts">
import { AppShell, Button, Card, Stack, useTheme } from "@ecoma-io/loom";

const { resolvedTheme, toggleTheme } = useTheme();
</script>

<template>
  <AppShell sidebar-aria-label="Runtime Trail navigation">
    <template #sidebar>
      <nav class="flex flex-col gap-1 p-2 text-sm">
        <span class="px-2 py-1 text-muted-foreground">Runtime</span>
        <span class="rounded-md bg-muted px-2 py-1 font-medium">Overview</span>
      </nav>
    </template>

    <template #header>
      <div class="flex w-full items-center justify-between gap-4">
        <p class="text-sm text-muted-foreground">
          Local developer observability
        </p>
        <Button variant="ghost" @click="toggleTheme">
          {{ resolvedTheme }} theme
        </Button>
      </div>
    </template>

    <main class="p-6">
      <Stack gap="lg" class="max-w-2xl">
        <Card title="Runtime Trail">
          <p class="text-sm leading-relaxed">
            A local developer observability and investigation runtime. This is
            the engineering foundation: the workspace, toolchain, architecture
            boundaries and gates are in place —
            <strong>no telemetry is ingested, queried, or rendered yet</strong>.
            The roadmap starts at OpenTelemetry ingestion.
          </p>
        </Card>

        <Card title="What exists today">
          <p class="text-sm leading-relaxed">
            The native core boots and answers <code>/healthz</code> and
            <code>/version</code> on <code>127.0.0.1:8599</code>. The UI and the
            MCP surface are peer shells over the same core; both will reach it
            through one Investigation API. See
            <code>docs/roadmap/phases.md</code> for what lands in which phase.
          </p>
        </Card>
      </Stack>
    </main>
  </AppShell>
</template>
