<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { SpanView } from "../api/investigation";
import { nanoSpanMs } from "../api/investigation";
import VirtualList from "./VirtualList.vue";

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

const tree = computed(() => buildTree(props.spans));

/** Which span ids are collapsed; a missing key means expanded (the default). */
const expandedById = ref<Record<string, boolean>>({});

// A new trace is a new set of collapsible rows: start every node expanded.
watch(
  () => props.spans,
  () => {
    expandedById.value = {};
  },
);

function isExpanded(spanId: string): boolean {
  return expandedById.value[spanId] ?? true;
}

function toggleExpanded(spanId: string): void {
  expandedById.value = {
    ...expandedById.value,
    [spanId]: !isExpanded(spanId),
  };
}

interface WaterfallRow {
  span: SpanView;
  depth: number;
  hasChildren: boolean;
  expanded: boolean;
}

/** Depth-first rows, skipping the subtrees of collapsed spans. */
const rows = computed<WaterfallRow[]>(() => {
  const out: WaterfallRow[] = [];
  const visit = (nodes: SpanNode[], depth: number): void => {
    for (const node of nodes) {
      const spanId = node.span.span.span_id;
      const expanded = isExpanded(spanId);
      const hasChildren = node.children.length > 0;
      out.push({ span: node.span, depth, hasChildren, expanded });
      if (expanded && hasChildren) visit(node.children, depth + 1);
    }
  };
  visit(tree.value, 0);
  return out;
});

const activeRow = ref(-1);

/**
 * ArrowRight/ArrowLeft expand and collapse the active row. The lazy Loom
 * keydown routine owns Home/End/PageUp/PageDown/Enter/arrows-through-rows;
 * this merged listener only routes the two tree keys, and only when the
 * event target is a row — a key typed inside a nested control (the chevron,
 * the select button) belongs to that control.
 */
function onRowsKeydown(event: KeyboardEvent): void {
  const target = event.target as Element | null;
  if (!target?.hasAttribute("data-virtual-index")) return;
  // Like the list's own keydown routine: with nothing active yet the first
  // row is the active one.
  const index = activeRow.value >= 0 ? activeRow.value : 0;
  const row = rows.value[index];
  if (row === undefined) return;
  const spanId = row.span.span.span_id;
  if (event.key === "ArrowRight" && row.hasChildren && !row.expanded) {
    event.preventDefault();
    toggleExpanded(spanId);
  } else if (event.key === "ArrowLeft" && row.hasChildren && row.expanded) {
    event.preventDefault();
    toggleExpanded(spanId);
  }
}

function onActivate(index: number): void {
  const row = rows.value[index];
  if (row === undefined) return;
  emit("select-span", row.span.span.span_id);
}

const traceStartNs = computed<bigint>(() => {
  let earliest: bigint | undefined;
  for (const v of props.spans) {
    const t = BigInt(v.span.start_time_unix_nano);
    if (earliest === undefined || t < earliest) earliest = t;
  }
  return earliest ?? 0n;
});

const totalDurationNs = computed<bigint>(() => {
  let maxEnd = 0n;
  for (const v of props.spans) {
    const end = BigInt(
      v.span.end_time_unix_nano ?? v.span.start_time_unix_nano,
    );
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
    BigInt(span.span.end_time_unix_nano ?? span.span.start_time_unix_nano) -
    BigInt(span.span.start_time_unix_nano);
  return Math.max(Number((durNs * 100n) / totalDurationNs.value), 0.5);
}
</script>

<template>
  <div v-if="spans.length === 0" class="text-sm text-muted-foreground">
    The envelope returned no spans.
  </div>
  <div v-else class="overflow-x-auto">
    <!-- Timing scale header -->
    <div
      class="flex items-center gap-2 border-b pb-1 text-xs font-medium text-muted-foreground"
    >
      <span class="w-44 shrink-0 pl-4">Span</span>
      <span class="w-16 shrink-0 text-right">Ms</span>
      <span class="flex-1 text-center">{{ totalDurationMs }}ms total</span>
    </div>

    <VirtualList
      v-model:active-index="activeRow"
      :items="rows"
      :item-height="32"
      label="Trace waterfall"
      class="max-h-96"
      @activate="onActivate"
      @keydown="onRowsKeydown"
    >
      <template #default="{ item, active }">
        <div
          class="flex h-full items-center gap-1 border-b text-sm"
          :style="{ paddingLeft: `${1 + item.depth * 1.25}rem` }"
        >
          <!-- A leaf row keeps a gutter the same width as the chevron, so
               names align whether or not the span has children. -->
          <button
            v-if="item.hasChildren"
            type="button"
            class="grid h-4 w-4 shrink-0 place-items-center rounded text-muted-foreground hover:bg-muted"
            :aria-expanded="item.expanded ? 'true' : 'false'"
            :aria-label="
              item.expanded
                ? `Collapse ${item.span.span.name}`
                : `Expand ${item.span.span.name}`
            "
            @click.stop="toggleExpanded(item.span.span.span_id)"
          >
            <svg
              viewBox="0 0 16 16"
              class="h-3.5 w-3.5 transition-transform"
              :class="item.expanded ? 'rotate-90' : ''"
              aria-hidden="true"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <path d="M6 4l4 4-4 4" />
            </svg>
          </button>
          <span v-else class="w-4 shrink-0" aria-hidden="true" />

          <button
            type="button"
            :class="[
              'flex h-full min-w-0 flex-1 cursor-pointer items-center gap-2 text-left',
              item.span.span.span_id === selectedSpanId
                ? 'bg-accent/50'
                : 'hover:bg-muted/50',
              active ? 'ring-1 ring-inset ring-muted-foreground/40' : '',
            ]"
            :aria-current="
              item.span.span.span_id === selectedSpanId ? 'true' : undefined
            "
            @click="emit('select-span', item.span.span.span_id)"
          >
            <span
              class="w-44 shrink-0 truncate"
              :title="item.span.span.span_id"
            >
              {{ item.span.span.name }}
            </span>
            <span class="w-16 shrink-0 text-right tabular-nums">
              {{
                nanoSpanMs(
                  item.span.span.start_time_unix_nano,
                  item.span.span.end_time_unix_nano,
                )
              }}
            </span>
            <span class="relative flex-1 h-5" aria-hidden="true">
              <span
                class="absolute h-3 rounded bg-primary/60"
                :style="{
                  left: `${barOffsetPct(item.span)}%`,
                  width: `${barWidthPct(item.span)}%`,
                }"
              />
            </span>
          </button>
        </div>
      </template>
    </VirtualList>
  </div>
</template>
