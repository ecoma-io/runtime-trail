<script setup lang="ts">
import { computed } from "vue";
import { Table, TableHead, TableRow, TableCell } from "@ecoma-io/loom";
import type { LogView } from "../api/investigation";

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
  return String(body);
}
</script>

<template>
  <div v-if="logs.length === 0" class="text-sm text-muted-foreground">
    The envelope returned no related logs.
  </div>
  <Table v-else caption="Related logs" density="compact">
    <template #default>
      <TableRow>
        <TableHead>Body</TableHead>
        <TableHead>Span</TableHead>
        <TableHead>Timestamp</TableHead>
      </TableRow>
      <TableRow
        v-for="(logView, index) in sortedLogs"
        :key="index"
        interactive
        :selected="
          selectedLogSpanId !== null &&
          selectedLogSpanId !== undefined &&
          logView.log.span_id === selectedLogSpanId
        "
        @activate="emit('select-log', logView.log.span_id ?? null)"
      >
        <TableCell>{{ bodyText(logView.log.body) }}</TableCell>
        <TableCell class="font-mono text-xs">{{
          logView.log.span_id ?? "—"
        }}</TableCell>
        <TableCell class="tabular-nums text-xs">{{
          logView.log.timestamp_unix_nano
        }}</TableCell>
      </TableRow>
    </template>
  </Table>
</template>
