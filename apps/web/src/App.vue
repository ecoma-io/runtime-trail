<script setup lang="ts">
import { AppShell, Button, Card, Stack, useTheme } from "@ecoma-io/loom";
import TraceInvestigation from "./components/TraceInvestigation.vue";

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
      <Stack gap="lg" class="max-w-3xl">
        <Card title="Runtime Trail">
          <p class="text-sm leading-relaxed">
            A local developer observability and investigation runtime. The
            engineering foundation — workspace, toolchain, architecture
            boundaries and gates — is in place, and
            <strong>telemetry ingestion is live</strong>: the core admits
            OTLP/HTTP and OTLP/gRPC exports into a bounded in-memory store under
            backpressure. The investigation screen renders the
            <strong>trace waterfall</strong>, <strong>related logs</strong>,
            <strong>logs ↔ trace navigation</strong>, the
            <strong>surrounding metrics window</strong>, and the
            <strong>correlated relations</strong> for one trace, queried over
            HTTP from the Investigation API on <code>127.0.0.1:8599</code>.
          </p>
        </Card>

        <TraceInvestigation />
      </Stack>
    </main>
  </AppShell>
</template>
