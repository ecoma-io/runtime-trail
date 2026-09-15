<script setup lang="ts">
import { ref, computed } from "vue";
import { Button, Card, Stack, Spinner, InlineError, Kbd } from "@ecoma-io/loom";
import { investigateTrace } from "../api/investigation";
import type { InvestigationEnvelope } from "../api/investigation";
import TraceWaterfall from "./TraceWaterfall.vue";
import RelatedLogs from "./RelatedLogs.vue";
import MetricsWindow from "./MetricsWindow.vue";
import CorrelatedRelations from "./CorrelatedRelations.vue";

const traceId = ref("00000000000000000000000000000000");
const spanId = ref("0000000000000000");
const envelope = ref<InvestigationEnvelope | null>(null);
const error = ref<string | null>(null);
const loading = ref(false);

/** The span currently selected in the waterfall or logs (span_id string). */
const selectedSpanId = ref<string | null>(null);
/** A log row navigation target: click a log → scroll to its span in the waterfall. */
const navigatedFromLog = ref<string | null>(null);

async function investigate(): Promise<void> {
  loading.value = true;
  error.value = null;
  envelope.value = null;
  selectedSpanId.value = null;
  navigatedFromLog.value = null;
  try {
    envelope.value = await investigateTrace({
      root_span: { span: { trace_id: traceId.value, span_id: spanId.value } },
    });
  } catch (err: unknown) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

function onSelectSpan(spanId: string): void {
  selectedSpanId.value = spanId;
}

function onSelectLog(logSpanId: string | null): void {
  if (logSpanId === null) return;
  selectedSpanId.value = logSpanId;
  navigatedFromLog.value = logSpanId;
}

const subjectRoot = computed(() => envelope.value?.subject.effective.root);
const evidence = computed(() => envelope.value?.evidence);
const correlated = computed(() => envelope.value?.correlated);
const execution = computed(() => envelope.value?.execution);

const hasSomeOutput = computed(
  () =>
    (evidence.value?.spans.length ?? 0) > 0 ||
    (evidence.value?.logs.length ?? 0) > 0 ||
    (evidence.value?.points.length ?? 0) > 0 ||
    (correlated.value?.relations.length ?? 0) > 0,
);
</script>

<template>
  <Card title="Investigate a trace">
    <template #default>
      <Stack gap="lg">
        <p class="text-sm text-muted-foreground">
          Enter a root span identity to investigate. The investigation API
          returns the trace waterfall, related logs, the surrounding metrics
          window, and correlated relations for exactly that subject.
        </p>

        <div class="flex flex-wrap items-end gap-3">
          <label for="trace-id" class="flex flex-col gap-1">
            <span class="text-xs font-medium"
              >Trace ID <Kbd class="ml-1">32 hex</Kbd></span
            >
            <input
              id="trace-id"
              v-model="traceId"
              class="w-[36ch] rounded border bg-background px-2 py-1 font-mono text-sm"
              placeholder="0000…0000 (32 hex)"
              maxlength="32"
              spellcheck="false"
            />
          </label>
          <label for="span-id" class="flex flex-col gap-1">
            <span class="text-xs font-medium"
              >Span ID <Kbd class="ml-1">16 hex</Kbd></span
            >
            <input
              id="span-id"
              v-model="spanId"
              class="w-[20ch] rounded border bg-background px-2 py-1 font-mono text-sm"
              placeholder="0000…0000 (16 hex)"
              maxlength="16"
              spellcheck="false"
            />
          </label>
          <Button variant="primary" :disabled="loading" @click="investigate">
            <Spinner v-if="loading" class="mr-1" />
            {{ loading ? "Investigating…" : "Investigate" }}
          </Button>
        </div>

        <!-- Error -->
        <InlineError v-if="error !== null" :message="error" />

        <!-- Subject -->
        <div
          v-if="subjectRoot !== undefined"
          class="rounded bg-muted/50 p-3 text-xs"
        >
          <div>
            <span class="font-medium">Resolved root:</span>
            {{ subjectRoot.name }}
            <span class="font-mono opacity-70">
              (trace {{ subjectRoot.trace_id.slice(0, 12) }}… span
              {{ subjectRoot.span_id.slice(0, 8) }}…)
            </span>
          </div>
          <div
            v-if="envelope?.subject.effective.notes.length"
            class="mt-1 text-muted-foreground"
          >
            Notes:
            {{
              envelope.subject.effective.notes
                .map((n) => `${n.kind}${n.name ? ` (${n.name})` : ""}`)
                .join("; ")
            }}
          </div>
        </div>

        <!-- Outputs -->
        <template v-if="hasSomeOutput">
          <!-- Trace Waterfall -->
          <Card title="Trace waterfall">
            <template #default>
              <TraceWaterfall
                :spans="evidence?.spans ?? []"
                :selected-span-id="selectedSpanId"
                @select-span="onSelectSpan"
              />
            </template>
          </Card>

          <!-- Related Logs -->
          <Card title="Related logs">
            <template #default>
              <div class="mb-1 text-xs text-muted-foreground">
                Click a log to navigate to its span in the waterfall.
              </div>
              <RelatedLogs
                :logs="evidence?.logs ?? []"
                :selected-log-span-id="selectedSpanId"
                @select-log="onSelectLog"
              />
            </template>
          </Card>

          <!-- Surrounding Metrics Window -->
          <Card title="Surrounding metrics window">
            <template #default>
              <MetricsWindow
                :points="evidence?.points ?? []"
                :flow-coverage="execution?.flow_coverage ?? []"
              />
            </template>
          </Card>

          <!-- Correlated Relations -->
          <Card title="Correlated relations">
            <template #default>
              <CorrelatedRelations :relations="correlated?.relations ?? []" />
            </template>
          </Card>
        </template>

        <!-- Honest empty state after fetch with no data -->
        <div
          v-if="envelope !== null && !hasSomeOutput"
          class="rounded bg-muted/50 p-4 text-sm text-muted-foreground"
        >
          The investigation returned no evidence or correlated relations for
          this root span.
        </div>
      </Stack>
    </template>
  </Card>
</template>
