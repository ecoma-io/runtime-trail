<script setup lang="ts">
import { computed } from "vue";
import type { SpanView } from "../api/investigation";
import { nanoSpanMs } from "../api/investigation";

const props = defineProps<{
  spans: SpanView[];
  selectedSpanId?: string | null;
}>();

const emit = defineEmits<{
  "select-span": [spanId: string];
}>();

interface SpanNode {
  span: SpanView;
  children: SpanNode[];
}

/** Build the span tree from a flat list using parent_span_id links. */
function buildTree(spans: SpanView[]): SpanNode[] {
  const byId = new Map<string, SpanNode>();
  for (const view of spans) {
    byId.set(view.span.span_id, { span: view, children: [] });
  }
  const roots: SpanNode[] = [];
  for (const node of byId.values()) {
    const pid = node.span.span.parent_span_id;
    const parent =
      pid !== null && pid !== undefined ? byId.get(pid) : undefined;
    if (parent !== undefined) {
      parent.children.push(node);
    } else {
      roots.push(node);
    }
  }
  const byStartTime = (a: SpanNode, b: SpanNode) =>
    BigInt(a.span.span.start_time_unix_nano) <
    BigInt(b.span.span.start_time_unix_nano)
      ? -1
      : 1;
  const sortAll = (nodes: SpanNode[]): void => {
    nodes.sort(byStartTime);
    for (const n of nodes) sortAll(n.children);
  };
  sortAll(roots);
  return roots;
}

/** Flatten a tree depth-first, recording the depth for indentation. */
function flatten(
  nodes: SpanNode[],
  depth: number,
): { span: SpanView; depth: number }[] {
  const out: { span: SpanView; depth: number }[] = [];
  for (const node of nodes) {
    out.push({ span: node.span, depth });
    out.push(...flatten(node.children, depth + 1));
  }
  return out;
}

const rows = computed(() => flatten(buildTree(props.spans), 0));

const traceStartNs = computed<bigint>(() => {
  let earliest: bigint | undefined;
  for (const v of props.spans) {
    const t = BigInt(v.span.start_time_unix_nano);
    if (earliest === undefined || t < earliest) earliest = t;
  }
  return earliest ?? 0n;
});

const totalDurationNs = computed<bigint>(() => {
  if (props.spans.length === 0) return 1n;
  let maxEnd = BigInt(0);
  for (const v of props.spans) {
    const end = BigInt(v.span.end_time_unix_nano);
    if (end > maxEnd) maxEnd = end;
  }
  const d = maxEnd - traceStartNs.value;
  return d > 0n ? d : 1n;
});

const totalDurationMs = computed(() =>
  Number(totalDurationNs.value / 1_000_000n),
);

function barOffsetPct(span: SpanView): number {
  const offsetNs = BigInt(span.span.start_time_unix_nano) - traceStartNs.value;
  return Number((offsetNs * 100n) / totalDurationNs.value);
}

function barWidthPct(span: SpanView): number {
  const durNs =
    BigInt(span.span.end_time_unix_nano) -
    BigInt(span.span.start_time_unix_nano);
  return Math.max(Number((durNs * 100n) / totalDurationNs.value), 0.5);
}
</script>

<template>
  <div v-if="spans.length === 0" class="text-sm text-muted-foreground">
    The envelope returned no spans.
  </div>
  <div v-else class="overflow-x-auto" role="list" aria-label="Trace waterfall">
    <!-- Timing scale header -->
    <div
      class="flex items-center gap-2 border-b pb-1 text-xs font-medium text-muted-foreground"
    >
      <span class="w-44 shrink-0">Span</span>
      <span class="w-16 shrink-0 text-right">Ms</span>
      <span class="flex-1 text-center">{{ totalDurationMs }}ms total</span>
    </div>

    <!-- Span rows -->
    <div
      v-for="row in rows"
      :key="row.span.span.span_id"
      role="listitem"
      class="border-b px-0 py-1 text-sm"
    >
      <button
        type="button"
        :class="[
          'flex w-full cursor-pointer items-center gap-2 text-left',
          row.span.span.span_id === selectedSpanId
            ? 'bg-accent/50'
            : 'hover:bg-muted/50',
        ]"
        :style="{ paddingLeft: `${1 + row.depth * 1.25}rem` }"
        @click="emit('select-span', row.span.span.span_id)"
      >
        <span class="w-44 shrink-0 truncate" :title="row.span.span.span_id">
          {{ row.span.span.name }}
        </span>
        <span class="w-16 shrink-0 text-right tabular-nums">
          {{
            nanoSpanMs(
              row.span.span.start_time_unix_nano,
              row.span.span.end_time_unix_nano,
            )
          }}
        </span>
        <span class="relative flex-1 h-5" aria-hidden="true">
          <span
            class="absolute h-3 rounded bg-primary/60"
            :style="{
              left: `${barOffsetPct(row.span)}%`,
              width: `${barWidthPct(row.span)}%`,
            }"
          />
        </span>
      </button>
    </div>
  </div>
</template>
