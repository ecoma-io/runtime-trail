// Minimal OTLP protobuf wire reader for the e2e fixtures; only the fields
// the specs assert on are decoded, everything else is skipped by length.
//
// Field numbers (proto3):
//   ExportTraceServiceRequest{ resource_spans = 1 }
//   ResourceSpans{ scope_spans = 2 }      ScopeSpans{ spans = 2 }
//   Span{ trace_id = 1 (bytes 16), span_id = 2 (bytes 8),
//         parent_span_id = 4 (bytes 8), name = 5 (string) }
//   ExportLogsServiceRequest{ resource_logs = 1 }
//   ResourceLogs{ scope_logs = 2 }        ScopeLogs{ log_records = 3 }
//   LogRecord{ trace_id = 5, span_id = 10 (bytes 8) }

export interface DecodedSpan {
  /** 32 hex chars. */
  traceId: string;
  /** 16 hex chars. */
  spanId: string;
  /** 16 hex chars, or null for a root span (empty parent bytes). */
  parentSpanId: string | null;
  name: string;
}

export interface DecodedLog {
  /** 32 hex chars, or null when the record carries no trace id. */
  traceId: string | null;
  /** 16 hex chars, or null when the record carries no span id. */
  spanId: string | null;
}

function readVarint(
  bytes: Uint8Array,
  at: number,
): { value: number; next: number } {
  let value = 0;
  let shift = 0;
  let i = at;
  while (i < bytes.length) {
    const byte = bytes[i];
    if (byte === undefined) break;
    value |= (byte & 0x7f) << shift;
    i += 1;
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return { value: value >>> 0, next: i };
}

function skipField(bytes: Uint8Array, wire: number, at: number): number {
  switch (wire) {
    case 0:
      return readVarint(bytes, at).next;
    case 1:
      return at + 8;
    case 2: {
      const { value, next } = readVarint(bytes, at);
      return next + value;
    }
    case 5:
      return at + 4;
    default:
      throw new Error(`unsupported protobuf wire type ${wire}`);
  }
}

/** Visit each length-delimited sub-message of `fieldNo`, with its bytes. */
function eachSubMessage(
  bytes: Uint8Array,
  fieldNo: number,
  visit: (sub: Uint8Array) => void,
): void {
  let at = 0;
  while (at < bytes.length) {
    const { value: tag, next } = readVarint(bytes, at);
    const field = tag >>> 3;
    const wire = tag & 7;
    if (field === fieldNo && wire === 2) {
      const { value: len, next: lenNext } = readVarint(bytes, next);
      visit(bytes.subarray(lenNext, lenNext + len));
      at = lenNext + len;
    } else {
      at = skipField(bytes, wire, next);
    }
  }
}

function toHex(bytes: Uint8Array): string {
  let out = "";
  for (const byte of bytes) out += byte.toString(16).padStart(2, "0");
  return out;
}

function decodeSpan(span: Uint8Array): DecodedSpan {
  let traceId = "";
  let spanId = "";
  let parentSpanId: string | null = null;
  let name = "";
  let at = 0;
  while (at < span.length) {
    const { value: tag, next } = readVarint(span, at);
    const field = tag >>> 3;
    const wire = tag & 7;
    if (wire === 2) {
      const { value: len, next: lenNext } = readVarint(span, next);
      const slice = span.subarray(lenNext, lenNext + len);
      switch (field) {
        case 1:
          traceId = toHex(slice);
          break;
        case 2:
          spanId = toHex(slice);
          break;
        case 4:
          parentSpanId = slice.length === 0 ? null : toHex(slice);
          break;
        case 5:
          name = new TextDecoder().decode(slice);
          break;
      }
      at = lenNext + len;
    } else {
      at = skipField(span, wire, next);
    }
  }
  return { traceId, spanId, parentSpanId, name };
}
export function decodeTraces(payload: Uint8Array): DecodedSpan[] {
  const spans: DecodedSpan[] = [];
  eachSubMessage(payload, 1, (resourceSpans) => {
    eachSubMessage(resourceSpans, 2, (scopeSpans) => {
      eachSubMessage(scopeSpans, 2, (span) => {
        spans.push(decodeSpan(span));
      });
    });
  });
  return spans;
}

function decodeLogRecord(record: Uint8Array): DecodedLog {
  let traceId: string | null = null;
  let spanId: string | null = null;
  let at = 0;
  while (at < record.length) {
    const { value: tag, next } = readVarint(record, at);
    const field = tag >>> 3;
    const wire = tag & 7;
    if (wire === 2) {
      const { value: len, next: lenNext } = readVarint(record, next);
      const slice = record.subarray(lenNext, lenNext + len);
      if (field === 5) traceId = toHex(slice);
      else if (field === 10) spanId = toHex(slice);
      at = lenNext + len;
    } else {
      at = skipField(record, wire, next);
    }
  }
  return { traceId, spanId };
}

export function decodeLogs(payload: Uint8Array): DecodedLog[] {
  const records: DecodedLog[] = [];
  eachSubMessage(payload, 1, (resourceLogs) => {
    eachSubMessage(resourceLogs, 2, (scopeLogs) => {
      eachSubMessage(scopeLogs, 3, (record) => {
        records.push(decodeLogRecord(record));
      });
    });
  });
  return records;
}
