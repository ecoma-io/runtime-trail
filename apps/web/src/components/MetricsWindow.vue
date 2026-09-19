<script setup lang="ts">
import { computed } from "vue";
import { Table, TableHead, TableRow, TableCell } from "@ecoma-io/loom";
import type { PointView, FlowCoverageEntry } from "../api/investigation";

const props = defineProps<{
  points: PointView[];
  flowCoverage: FlowCoverageEntry[];
}>();

const metricWindow = computed(() =>
  props.flowCoverage.find((e) => e.kind === "metric_window"),
);

const sortedPoints = computed(() =>
  [...props.points].sort((a, b) => {
    if (
      a.point.time_unix_nano === undefined ||
      b.point.time_unix_nano === undefined
    )
      return 0;
    return Number(
      BigInt(a.point.time_unix_nano) - BigInt(b.point.time_unix_nano),
    );
  }),
);

function pointValue(value: PointView["point"]["value"]): string {
  if (value === undefined) return "—";
  return "int" in value ? String(value.int) : String(value.double);
}
</script>

<template>
  <div v-if="points.length === 0" class="text-sm text-muted-foreground">
    The envelope returned no surrounding metric points.
  </div>
  <template v-else>
    <Table caption="Surrounding metric points" density="compact">
      <template #default>
        <TableRow>
          <TableHead>Stream</TableHead>
          <TableHead>Time (ns)</TableHead>
          <TableHead>Value</TableHead>
        </TableRow>
        <TableRow v-for="(pointView, index) in sortedPoints" :key="index">
          <TableCell>{{ pointView.stream.name }}</TableCell>
          <TableCell class="tabular-nums text-xs">
            {{ pointView.point.time_unix_nano ?? "—" }}
          </TableCell>
          <TableCell class="tabular-nums">
            {{ pointValue(pointView.point.value) }}
          </TableCell>
        </TableRow>
      </template>
    </Table>
    <div class="mt-2 rounded bg-muted/50 p-2 text-xs text-muted-foreground">
      <template
        v-if="
          metricWindow !== undefined &&
          metricWindow.asked !== undefined &&
          metricWindow.resident !== undefined
        "
      >
        Metric window: asked
        {{ metricWindow.asked.from }}–{{ metricWindow.asked.to }}, resident
        points span {{ metricWindow.resident.from }}–{{
          metricWindow.resident.to
        }}
      </template>
      <template v-else>
        Metric window: un-bounded — the runtime did not state a window for these
        points.
      </template>
    </div>
  </template>
</template>
