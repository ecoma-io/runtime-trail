<script setup lang="ts">
import { computed, ref } from "vue";
import type { LogView } from "../api/investigation";
import VirtualList from "./VirtualList.vue";

const props = defineProps<{
  logs: LogView[];
  selectedLogSpanId?: string | null;
}>();

const emit = defineEmits<{
  "select-log": [spanId: string | null];
}>();

function toNano(val: string | number | bigint | null | undefined): bigint {
  if (val == null) return -1n; // nulls sort first
  return BigInt(val);
}

const sortedLogs = computed(() =>
  [...props.logs].sort((a, b) =>
    Number(
      toNano(a.log.timestamp_unix_nano) - toNano(b.log.timestamp_unix_nano),
    ),
  ),
);

function bodyText(body: LogView["log"]["body"]): string {
  if (body === undefined || body === null) return "";
  if (typeof body === "string") return body;
  if (typeof body === "number" || typeof body === "boolean") {
    return String(body);
  }
  // Structured values (arrays and key-value lists) render as JSON, so an
  // empty array or object stays visible as `[]` / `{}` — never confused
  // with an absent body, which renders as "".
  return JSON.stringify(body);
}

const activeRow = ref(-1);

function onActivate(index: number): void {
  const logView = sortedLogs.value[index];
  if (logView === undefined) return;
  emit("select-log", logView.log.span_id ?? null);
}

/** The three columns, shared verbatim by the header and every virtualized row. */
const GRID_COLUMNS = "grid-cols-[minmax(0,1fr)_26ch_34ch]";
</script>

<template>
  <div v-if="logs.length === 0" class="text-sm text-muted-foreground">
    The envelope returned no related logs.
  </div>
  <div v-else>
    <div
      :class="[
        'grid items-center gap-3 border-b px-2 pb-1 text-xs font-medium text-muted-foreground',
        GRID_COLUMNS,
      ]"
    >
      <span>Body</span>
      <span>Span</span>
      <span>Timestamp</span>
    </div>
    <VirtualList
      v-model:active-index="activeRow"
      :items="sortedLogs"
      :item-height="32"
      label="Related logs"
      class="max-h-96"
      @activate="onActivate"
    >
      <template #default="{ item: logView }">
        <button
          type="button"
          :class="[
            'grid h-full w-full items-center gap-3 border-b px-2 text-left text-sm',
            GRID_COLUMNS,
            selectedLogSpanId !== null &&
            selectedLogSpanId !== undefined &&
            logView.log.span_id === selectedLogSpanId
              ? 'bg-accent/50'
              : 'hover:bg-muted/50',
          ]"
          :aria-current="
            selectedLogSpanId !== null &&
            selectedLogSpanId !== undefined &&
            logView.log.span_id === selectedLogSpanId
              ? 'true'
              : undefined
          "
          @click="emit('select-log', logView.log.span_id ?? null)"
        >
          <span class="truncate">{{ bodyText(logView.log.body) }}</span>
          <span class="truncate font-mono text-xs">
            {{ logView.log.span_id ?? "—" }}
          </span>
          <span class="truncate tabular-nums text-xs">
            {{ logView.log.timestamp_unix_nano }}
          </span>
        </button>
      </template>
    </VirtualList>
  </div>
</template>
