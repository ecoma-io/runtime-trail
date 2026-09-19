<script setup lang="ts">
import { Table, TableHead, TableRow, TableCell, Badge } from "@ecoma-io/loom";
import type { EvidenceFact, Relation, EntityId } from "../api/investigation";

defineProps<{
  relations: Relation[];
}>();

function entityText(entity: EntityId): string {
  if ("span" in entity)
    return `${entity.span.trace_id.slice(0, 8)}…/${entity.span.span_id.slice(0, 8)}`;
  return `#${entity.assigned}`;
}

function factText(fact: EvidenceFact): string {
  if (fact.value === null) return `${fact.field}=null`;
  if (typeof fact.value === "string") return `${fact.field}="${fact.value}"`;
  if (typeof fact.value === "number" || typeof fact.value === "boolean") {
    return `${fact.field}=${String(fact.value)}`;
  }
  // Structured values (arrays and key-value lists) render as JSON, so
  // they stay distinguishable from `field=null` (absent) on the wire.
  return `${fact.field}=${JSON.stringify(fact.value)}`;
}
</script>

<template>
  <div v-if="relations.length === 0" class="text-sm text-muted-foreground">
    The envelope returned no correlated relations.
  </div>
  <div v-else class="overflow-x-auto">
    <Table caption="Correlated relations" density="compact">
      <template #default>
        <TableRow>
          <TableHead>Type</TableHead>
          <TableHead>From</TableHead>
          <TableHead>To</TableHead>
          <TableHead>Facts</TableHead>
          <TableHead>Strategy</TableHead>
        </TableRow>
        <TableRow v-for="(relation, index) in relations" :key="index">
          <TableCell>
            <Badge>{{ relation.type }}</Badge>
          </TableCell>
          <TableCell class="text-xs">
            <span class="uppercase opacity-70">{{ relation.from.kind }}</span>
            {{ entityText(relation.from.entity) }}
          </TableCell>
          <TableCell class="text-xs">
            <span class="uppercase opacity-70">{{ relation.to.kind }}</span>
            {{ entityText(relation.to.entity) }}
          </TableCell>
          <TableCell class="text-xs">
            {{ relation.facts.map(factText).join("; ") || "—" }}
          </TableCell>
          <TableCell class="text-xs">
            {{ relation.strategy.name }}@{{ relation.strategy.version }}
          </TableCell>
        </TableRow>
      </template>
    </Table>
  </div>
</template>
