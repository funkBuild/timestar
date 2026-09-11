# Derived Query API

**Endpoint:** `POST /derived`
**Content-Type:** `application/json`

Execute multi-query expressions, anomaly detection, and forecasting. Sub-queries are executed in parallel, aligned to common timestamps, and combined via a formula.

## Basic Derived Query

Combine two queries with a formula:

```bash
curl -X POST http://localhost:8086/derived \
  -H "Content-Type: application/json" \
  -d '{
    "queries": {
      "a": "avg:system(bytes_sent){host:server-01}",
      "b": "avg:system(bytes_recv){host:server-01}"
    },
    "formula": "a + b",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

## Single Query with Formula

Apply a transform to a single query:

```bash
curl -X POST http://localhost:8086/derived \
  -H "Content-Type: application/json" \
  -d '{
    "queries": {
      "a": "avg:temperature(value){location:us-west}"
    },
    "formula": "a * 1.8 + 32",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

## Anomaly Detection

Use the `anomalies()` function in the formula:

```bash
curl -X POST http://localhost:8086/derived \
  -H "Content-Type: application/json" \
  -d '{
    "queries": {
      "a": "avg:cpu(usage){host:server-01}"
    },
    "formula": "anomalies(a, '\''basic'\'', 2)",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

See [Anomaly Detection](anomaly-detection.md) for algorithm details.

## Forecasting

Use the `forecast()` function in the formula:

```bash
curl -X POST http://localhost:8086/derived \
  -H "Content-Type: application/json" \
  -d '{
    "queries": {
      "a": "avg:temperature(value){location:us-west}"
    },
    "formula": "forecast(a, '\''linear'\'', 2)",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

See [Forecasting](forecasting.md) for algorithm details.

## Formula Syntax

Formulas reference sub-queries by name and support:

- **Arithmetic:** `a + b`, `a - b`, `a * b`, `a / b`
- **Constants:** `a * 1.8 + 32`
- **Functions:** `abs(a)`, `rate(a)`, `rolling_avg(a, 10)`
- **Nesting:** `abs(a - b) / max(a, b) * 100`

See [Expression Functions](expression-functions.md) for the full function list.

### Sub-queries must be numeric

A formula is arithmetic, so every sub-query it references must resolve to a
numeric field (float or integer). A **string or boolean** sub-query field is
rejected with `400`:

```json
{"status": "error", "error_code": "QUERY_ERROR",
 "message": "Sub-query 'a' returned non-numeric values",
 "error": "Sub-query 'a' returned non-numeric values"}
```

This applies to `anomalies(a)` and `forecast(a)` as well — they run the same
sub-query machinery.

Booleans used to be coerced to `1.0`/`0.0` here, so `avg:door(open) * 100` was
accepted as an "uptime %". That answer was wrong whenever an
`aggregationInterval` was set: booleans are non-numeric (see CLAUDE.md
"Non-Numeric Fields in Queries"), so the sub-query returns LATEST-per-bucket —
the state at the end of each bucket — not the fraction of the bucket spent
`true`. The formula then reported `0` or `100` where the user expected `60`.
Rejecting is deliberate: there is currently **no** supported way to compute a
boolean duty cycle through `/derived`.

## Per-series fan-out

`forecast()` and `anomalies()` run **once per series** their sub-query resolves
to, and return **one group per series**. No flag is needed: this is what the
response schema's `group_tags` and `series_count` have always described.

Before this was wired, a sub-query resolving to more than one series was refused
outright:

```json
{"status":"error","error":"Sub-query 'a' returned 2 series but derived queries require exactly one series. Add more specific scope filters to narrow the result."}
```

and a sub-query naming several **fields** was worse than refused — it silently
forecast the first field and reported `series_count: 1`, with nothing in the
response naming the field it chose.

### What a group is

A group's identity is its **(tag set, field)** key. Both axes count:

- a `by {tag}` clause splits the leg by tag set, one group per distinct set;
- naming *n* fields — `(f1,f2,f3)`, or `()` over a measurement holding three —
  splits it by field as well, one group per field.

A scope filter (`{dev:DEV-A}`) narrows which series match but does **not** label
them: only a `by {tag}` key survives into a series' tags. So
`avg:m(v){dev:A}` yields a group with **no** tags, while
`avg:m(v){dev:A} by {dev}` yields `["dev=A"]`.

The key is the same one the `multiSeries` arithmetic path pairs on — see
[Multi-series arithmetic](#multi-series-arithmetic), which additionally drops
the field from the key when it carries no information.

### Ordering

Groups come back in a **total order that is a pure function of the input**, so
the same request always answers in the same sequence regardless of which shard
replied first:

1. **tag set ascending** — lexicographic over the `(key, value)` pairs, so one
   device's entries stay contiguous;
2. then **field rank** — the order the fields were named in the query string for
   an explicit list, ascending field name for `()`;
3. then **field name**, which is what actually separates two different fields
   sharing a rank (two series each rank their own first field 0, so without this
   the tie fell through to arrival order and the sequence flipped when the
   shards answered in a different order);
4. then **input index**, so two series with an identical tag set *and* an
   identical field name still cannot swap places.

Tags sort before field because the consumer is per-device forecasting: grouping
the output by device is the useful adjacency.

### Group labels and `_field=`

`group_tags` is the group's tag map flattened to `"key=value"` in ascending key
order — then, **only when the result spans more than one distinct field**, the
synthetic entry `"_field=<name>"`, always **last**.

- A **single-field** result carries no field label at all, so every request that
  worked before fan-out existed gets byte-identical `group_tags`.
- The entry is appended rather than sorted in among the tags because it is not a
  tag: keeping it last leaves the tag sequence a client already parses
  untouched, and makes the synthetic entry trivially separable.
- The leading underscore follows **InfluxDB's reserved-column convention**
  (`_field`, `_measurement`), which this project already uses on the write API.
  The unprefixed `field=` this started as collided with a real tag **key** named
  `field` — and `by {field}` is in the reporter's own vocabulary, so the
  collision is reachable, not theoretical. It produced `group_tags` like
  `["field=TAGVAL", "field=f1"]`, which a client parsing `"k=v"` into a map
  silently reduces to one entry.
- **Pathological case, documented rather than defended against:** a measurement
  carrying a tag literally named `_field` still produces two `"_field="` entries
  on a multi-field result. The tag one comes first, in its tag position; the
  synthetic one is always last. **Position is the only discriminator**, which is
  why the synthetic entry's position is a contract and not an implementation
  detail.

### The five shapes

Against a `motor.vibration` measurement with fields `f1,f2,f3` and a `deviceId`
tag over `DEV-A`/`DEV-B`, with `formula: "forecast(a,'linear',2)"`:

| Leg | Groups | Labels | Note |
|-----|--------|--------|------|
| `(f1){deviceId:DEV-A}` | 1 | `[]` | one series; unchanged from before |
| `(f1,f2,f3){deviceId:DEV-A}` | **3** | `_field=f1` / `_field=f2` / `_field=f3` | was: silently forecast `f1` only |
| `(f1){}` | 1 | `[]` | the **cross-series merge**; unchanged — see below |
| `(f1){} by {deviceId}` | **2** | `deviceId=DEV-A` / `deviceId=DEV-B` | was: HTTP 400 |
| `(f1,f2,f3){} by {field}` | 0 | — | `field` is not a tag key; unchanged |

The last row is the canonical `/query` rule, not a `/derived` one: a grouping key
that no discovered series carries returns `[]` (see CLAUDE.md, "Unknown
`by {tag}` keys"), and `/derived` faithfully relays the empty result as an empty
success.

### An unscoped, ungrouped leg is one group, not many

`avg:m(f1){}` over two devices resolves to **one** series and therefore **one**
group. That is `POST /query`'s own cross-series rule — aggregate across series at
equal timestamps — and `/derived` inherits it rather than overriding it. Fan-out
splits what the query layer returns as separate series; it does not un-merge what
the query layer merged.

What that one series *contains* depends on the `aggregationInterval`, and the
difference is large enough to be worth stating:

- **With an interval**, the two devices' samples land in the same bucket and are
  averaged: a genuine **fleet average**. Two devices reading 10 and 90 give one
  series of 50.
- **Without an interval**, aggregation is per-timestamp — and if the devices
  sample at different offsets (`:00` and `:30`) they share no timestamp, so the
  cross-series aggregate degenerates to an interleaved **raw union**:
  `[10, 90, 10, 90, …]`. This is canonical `/query` behaviour, not a bug.

Forecasting the second shape fits a **sawtooth**, and it will report success
while doing so (a measured `r_squared` of 8.0e-5 with a residual standard
deviation of 40 around values that are really either 10 or 90). If you want one
answer per device, ask for one: add `by {deviceId}` and take two groups. If you
want a fleet average, set an `aggregationInterval` so there is something to
average.

### Interaction with `aggregationInterval`

The request's `aggregationInterval` now reaches `forecast()` and `anomalies()`
sub-queries. It previously did not — only a plain arithmetic formula honoured it
— so the same request body bucketed `a * 1` to 120 daily points while
`forecast(a,…)` fitted 2,880 raw hourly ones and reported success. Setting the
interval and omitting it produced bit-identical forecast output.

This is a **behaviour change** for any caller that was setting an interval on a
`forecast()`/`anomalies()` request: it now takes effect, so `historical_points`
falls to the bucket count and the horizon (`duration / interval`) changes with
it. A caller that wants raw resolution should omit the interval.

`aggregationInterval` accepts the same two JSON spellings as `POST /query`: a
JSON **number** is nanoseconds, a JSON **string** is either a unit suffix
(`"5m"`, `"1h"`) or a bare numeric string, which also means nanoseconds. All
three forms agree. The numeric spelling is newly accepted here — `/derived`
previously rejected it with a `400` parse error (`expected_quote`) even though it
has always been documented as `string/uint64`. Only bodies that were previously
**rejected** changed meaning, so no working client can break.

### Counters and limits

See [Declined groups](#declined-groups) for what happens to a group with too
little data, and [Fan-out limits](#fan-out-limits) for the three bounds that
guard a leg.

## Multi-series arithmetic

By default an arithmetic formula requires each sub-query to resolve to exactly
one series; a sub-query matching several is a `400`:

```json
{"status":"error","error_code":"QUERY_ERROR",
 "message":"Sub-query 'a' returned 3 series but derived queries require exactly one series. Add more specific scope filters to narrow the result.",
 "error":"Sub-query 'a' returned 3 series but derived queries require exactly one series. Add more specific scope filters to narrow the result."}
```

Set **`"multiSeries": true`** (protobuf: `DerivedQueryRequest.multi_series`) and
the formula is instead evaluated **once per group**, so `a / b` over a fleet is
one request instead of one per device:

```json
{
  "queries": {
    "a": "avg:netin(bytes){} by {deviceId}",
    "b": "avg:netout(bytes){} by {deviceId}"
  },
  "formula": "a / b",
  "startTime": 1704067200000000000,
  "endTime": 1704070800000000000,
  "multiSeries": true
}
```

```json
{
  "status": "success",
  "timestamps": [],
  "values": [],
  "series": [
    {"group_tags": ["deviceId=DEV-A"], "timestamps": [...], "values": [...]},
    {"group_tags": ["deviceId=DEV-B"], "timestamps": [...], "values": [...]}
  ],
  "formula": "a / b",
  "statistics": {"point_count": 240, "group_count": 2, "sub_queries_executed": 2, "execution_time_ms": 12.1}
}
```

### The rules

**Pairing.** A group's identity is its **(tag set, field)** key — the same key
`forecast()` and `anomalies()` fan out on. Sub-queries are paired key by key and
the formula runs over each pair independently. In particular `a + b` works on a
**per-field** basis: if both sides name the same fields, each field is computed
against its own counterpart.

**Cross-measurement pairing.** When **every** sub-query resolves to exactly one
distinct field name, the field carries no information and the key is the **tag
set alone**. That is what makes the obvious cross-measurement query work:

```json
{"queries": {"a": "avg:cpu(user){} by {host}", "b": "avg:mem(used){} by {host}"},
 "formula": "a / b", "multiSeries": true, "...": "..."}
```

one series per host on each side, paired by host. With the field in the key this
was a `400` no caller could act on — no scope filter renames a field, and
reducing either side to one series is the very thing the flag exists to stop
requiring. As soon as one sub-query names two fields (`avg:m(rx,tx)`), the field
is back in the key and per-field pairing applies as above. The `_field=` label
rule is unchanged: a tags-only result spans one field and carries no label.

The switch is decided by the fields a sub-query **resolves to**, not by the query
text, so an all-fields leg (`avg:cpu(){}`) follows the stored data: while `cpu`
holds one field, `avg:cpu(){} by {host} / avg:mem(){} by {host}` pairs on the tag
set and answers `200`; once a second field is written to `cpu`, the *identical*
request body pairs on `(tags, field)`, the two sides' key sets no longer match
and it becomes a `400`. It fails loudly rather than answering something else —
name the fields explicitly (`avg:cpu(user){}`) if you need the result to be
independent of what else the measurement collects.

**Broadcast.** A sub-query resolving to **exactly one** series is paired with
*every* group of the others — provided its own tag set is **empty**, or a
**subset** of every output group's. That is what makes
`per_device_bytes / fleet_total` (an untagged total) mean what it looks like it
means, and a sub-query scoped to a tag the output groups all share
(`{rack:R1} by {rack}` against `by {deviceId,rack}`) broadcasts for the same
reason. Every formula that worked before this flag keeps working unchanged.

A **tagged** single series that is not a subset is a `400`, not a broadcast:

```
a = avg:num(v){} by {dev}            -> dev=DEV-A, DEV-B, DEV-C
b = avg:den(v){dev:DEV-A} by {dev}   -> one series, tagged dev=DEV-A
```

Broadcasting `b` here would answer `200` with a group **labelled** `dev=DEV-B`
whose value is `num[DEV-B] / den[DEV-A]` — a pairing that never happened, from
exactly the over-narrow scope filter the mismatch error exists to catch. The
message names the offending tags and the groups they cannot cover.

The **field** is held to the analogue of that rule whenever it is part of the
pairing key (see *Cross-measurement pairing* above). The single series may
broadcast when its field is named by **no** output group — the common scalar
denominator, `avg:net(rx,tx){} by {host}` over `avg:total(bytes){}`, which keeps
working and is computed per field (`rx/bytes`, `tx/bytes`) — or by **all** of
them. A field matching **some** output groups but not others is a `400`:

```
a = avg:net(rx,tx){} by {host}   -> (host=h1, rx), (host=h1, tx)
b = avg:net2(rx){} by {host}     -> one series, field rx
```

Its tags match both groups, but broadcasting it would answer `200` with a group
labelled `_field=tx` whose value is `net.tx / net2.rx` — the same false claim as
the tagged case, one axis over. The remedy is different, so the message is too:
name the same fields on both sides, or reduce the other sub-queries to the field
this one names.

**This refuses one query a caller can reasonably want, and that is deliberate.**
The "named by **no** output key" arm above admits a scalar denominator from
another measurement — but only while its field name differs from every field on
the numerator. Rename nothing and the same query is a `400` purely because of a
name collision:

```
a = avg:net(rx,tx){} by {host}   -> (host=h1, rx), (host=h1, tx)
b = avg:lim(rx){}                -> one series, field rx   ->  400, "some but not all"
```

`lim.rx` is a per-host limit meant to divide both `rx` and `tx`, and there is no
scope filter that renames a field, so the caller's only remedy is to name `rx`
and `tx` on both sides or split the request in two. The alternative — treating a
collision as a broadcast — cannot be told apart from the genuinely wrong pairing
in the `net2` example above, since the two are the *same shape* on the wire. The
conservative choice is to refuse both and say why, rather than to answer one of
them wrongly.

**No fan-out, no subset test.** The rule above guards *one* series standing in
for *many* it does not describe. When **every** sub-query resolves to exactly one
series there is no fan-out and nothing to stand in for, so they pair regardless
of tags — comparing two named hosts is a legitimate query and answers exactly as
it does without the flag:

```json
{"queries": {"a": "avg:cpu(v){host:web1} by {host}", "b": "avg:cpu(v){host:web2} by {host}"},
 "formula": "a - b", "multiSeries": true, "...": "..."}
```

What the flag adds to that answer is a *label*, and `host=web1` would assert the
result came from a series that supplied only half of it. So the single group
carries **empty `group_tags`** whenever the sub-queries' tag sets differ — which
is exactly what the no-flag flat answer already says, since it carries no labels
at all. If every sub-query agrees on the tag set (`avg:in(v){} by {host}` over
`avg:out(v){} by {host}`, both resolving to `host=web1`), the group keeps those
tags.

**Mismatch is an error, never a silent intersection.** If two sub-queries each
resolve to more than one series and their key sets differ, the request is a
`400` naming the difference in both directions:

```json
{"status":"error","error_code":"QUERY_ERROR",
 "message":"Sub-queries 'a' and 'b' resolved to different series: 1 (deviceId=DEV-C) in 'a' but not 'b', 1 (deviceId=DEV-D) in 'b' but not 'a'. ...",
 "error":"Sub-queries 'a' and 'b' resolved to different series: 1 (deviceId=DEV-C) in 'a' but not 'b', 1 (deviceId=DEV-D) in 'b' but not 'a'. ..."}
```

(Each key is listed as its tags, plus `_field=<name>` when the field is part of
the pairing key — see *Cross-measurement pairing* above.)

Quietly answering for `{DEV-A, DEV-B}` would report success while omitting half
the question.

**Order.** Groups come back in the order of the **reference** sub-query — the
first one, by name, that resolves to more than one series (or simply the first
when none does). Each sub-query's own order is the fan-out
[ordering rule](#ordering), so the output inherits that; taking it from one named
sub-query is what keeps the sequence deterministic when two sub-queries disagree
about field order (`a = m(x,y)` against `b = m(y,x)`).

**Group labels.** Identical to the fan-out rule — see
[Group labels and `_field=`](#group-labels-and-_field). `group_tags` is the
group's tags flattened to `"key=value"` in ascending key order, followed, only
when the result spans more than one distinct field, by the synthetic
`"_field=<name>"` entry, always last.

**An empty sub-query is an empty result,** not a mismatch: if any referenced
sub-query resolves to no series at all, the response is an empty success — the
same answer the single-series path gives.

### Why it is opt-in

`DerivedQueryResponse` is a flat `timestamps`/`values` pair. A client that has
not been taught about `series` would read a multi-group answer as an *empty*
one — a silent wrong answer. So without the flag the old `400` stands, exactly
as before.

> **The one asterisk on "exactly as before".** Response bodies really are
> byte-identical for any request that does not set the flag. The **request**
> surface widened once: a **numeric** `aggregationInterval` is now accepted,
> where it used to be a `400` parse error (`expected_quote`) despite always
> being documented as `string/uint64`. Only previously-**rejected** bodies
> changed meaning, so no working client can break — but note it if you are
> diffing behaviour against a pre-campaign build.

With the flag set, `series` is the **whole** answer at every group count — one
group included — and the flat `timestamps`/`values` come back as empty arrays.
The migration is therefore one step: set the flag, read `series`. (An earlier
revision also filled the flat columns for a one-group answer so a client could
adopt the flag before the array. That carried every point **twice** — a measured
56.85 MB body became 113.71 MB, and in protobuf the FFOR/ALP compression ran over
the same points twice — with nothing to cap it, since a one-group result is
exempt from the point budget below.)

The protobuf encoding carries the same data: `DerivedQueryResponse.series`
(field 10) is a `repeated DerivedSeries`, each carrying `group_tags` plus
FFOR/ALP-compressed timestamps and values, exactly like the flat columns.

It cannot, however, mirror the *presence* rule for an **empty** answer. In JSON
`series` and `group_count` are `std::optional`, so `"series": []` with
`"group_count": 0` is distinguishable from a server that does not know the flag
(both keys absent). Proto3 has no such distinction: an empty `repeated` field
and a `group_count` of `0` are simply not on the wire, so "flag set, nothing
matched" and "flag ignored" encode identically. A protobuf client that needs to
know whether the flag took effect must therefore establish it some other way —
by version, or by a request it knows produces groups.

### Response shape under the flag

| | without `multiSeries` | with `multiSeries` |
|---|---|---|
| `timestamps` / `values` | the answer | `[]`, always |
| `series` | key absent | present, always — `[]` when nothing matched |
| `statistics.group_count` | key absent | number of groups (`0` when nothing matched) |

`group_tags` is empty (`[]`) for a group nothing distinguishes: an untagged
series, or the single group of a no-fan-out result whose sub-queries disagree
about their tags (see *Broadcast* above).

`series` is **absent** without the flag and **`[]`** with it when nothing
matched, so a client never has to read `undefined` as a third state.

`statistics.point_count` is the total across groups, and
`statistics.points_dropped_due_to_alignment` is likewise a **sum**: every group
is aligned independently, so a timestamp missing from one group's sub-queries is
counted once for that group. It is a total, not a per-group figure.

Duplicated JSON keys in one request body are **last-wins**
(`{"multiSeries":false,"multiSeries":true}` opts in), as they are for every other
key.

### Point budget under the flag

A multi-series result is bounded by the same **250,000-point** budget the
`forecast()`/`anomalies()` fan-out uses (`maxFanOutPoints`), counting
`series × points-per-series` — i.e. the points the response actually carries.
Exceeding it is a `400` saying how many points the query *would* produce and
suggesting a coarser (or added) `aggregationInterval`, a narrower time range or a
narrower scope.
A result with a **single** group is exempt, so a query that succeeded before the
flag existed cannot start failing because of it. The per-sub-query series cap
(`maxSeriesPerLeg`, default 200) applies here as it does to the fan-out.

The budget is checked **before** each group is built, and again afterwards as a
backstop. This matters because an arithmetic formula **does** fabricate points:
with an `aggregationInterval`, alignment resamples onto a dense grid running from
the first aligned timestamp to the last, so four stored points over a 30-day
window at `"1s"` become 2,592,001 emitted points (a measured 56.85 MB body and
two reactor stalls). Checking only afterwards meant the first group was never
checked at all and was materialised in full — 10.4× the budget — before being
refused.

The pre-check counts what the group would **actually** emit: the sub-queries'
shared timestamps are counted by walking their (already in-memory, already
sorted) timestamps, and the resample grid is projected from the first and last of
*those*. It is therefore neither an over- nor an under-statement, which is the
point — a query refused for 299,999 points that would in fact have returned 2,000
is as wrong an answer as one that is admitted and then stalls the reactor. Both
happened when the grid was sized over the whole interval spanned by the
sub-queries rather than over the timestamps they share.

The forecast output bound (`maxForecastOutputPoints`) does **not** apply here: it
counts a forecast horizon, and an arithmetic formula has none. That is *not* the
same as saying the path fabricates nothing — see the paragraph above; what bounds
it is `maxFanOutPoints` and its pre-check. A **single-group** result is exempt
from that pre-check and so is unbounded, exactly as the single-series path has
always been: bucketing four points at `"1s"` over 30 days returns 2,592,001
points with or without the flag.

## Request Parameters

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `queries` | object | yes | Map of name to query string (e.g., `{"a": "avg:cpu(usage){}"}`) |
| `formula` | string | yes | Expression combining sub-query results |
| `startTime` | uint64 | yes | Start time in nanoseconds |
| `endTime` | uint64 | yes | End time in nanoseconds |
| `aggregationInterval` | string/uint64 | no | Time bucket interval. A JSON number is nanoseconds; a JSON string is a unit suffix (`"5m"`) or a bare numeric string, also nanoseconds. Applies to **every** sub-query, `forecast()`/`anomalies()` included — see [Interaction with `aggregationInterval`](#interaction-with-aggregationinterval). |
| `multiSeries` | bool | no | Opt in to per-group evaluation of an arithmetic formula — see [Multi-series arithmetic](#multi-series-arithmetic). Default `false`. `forecast()`/`anomalies()` fan out without it. |

Unknown keys are rejected: the request body is parsed strictly, so a misspelled
`"multiseries"` is a `400` rather than a silently ignored flag. Duplicated keys
are last-wins.

## Response

**Derived query (200):**
```json
{
  "status": "success",
  "timestamps": [1704067200000000000, 1704067500000000000],
  "values": [45.2, 46.8],
  "formula": "a + b",
  "statistics": {
    "point_count": 2,
    "execution_time_ms": 45.2,
    "sub_queries_executed": 2,
    "points_dropped_due_to_alignment": 0
  }
}
```

**Anomaly detection (200):**
```json
{
  "status": "success",
  "times": [1704067200000000000, 1704067500000000000],
  "series": [
    {"piece": "raw", "group_tags": {}, "values": [23.5, 24.2]},
    {"piece": "upper", "group_tags": {}, "values": [26.0, 26.1]},
    {"piece": "lower", "group_tags": {}, "values": [21.0, 21.1]},
    {"piece": "score", "group_tags": {}, "values": [0.0, 0.0], "alert_value": 0.0}
  ],
  "statistics": {
    "algorithm": "basic",
    "bounds": 2.0,
    "anomaly_count": 0,
    "total_points": 2,
    "execution_time_ms": 32.1
  }
}
```

`total_points` counts the **real** observations across the answered groups, not
the width of the shared axis: under fan-out a group carries `null` wherever it
has no sample, and those slots are not its data.

**Forecast (200):**
```json
{
  "status": "success",
  "times": [1704067200000000000, 1704067500000000000, 1704067800000000000],
  "forecast_start_index": 2,
  "series": [
    {"piece": "past", "group_tags": {}, "values": [23.5, 24.2, null]},
    {"piece": "forecast", "group_tags": {}, "values": [null, null, 24.8]},
    {"piece": "upper", "group_tags": {}, "values": [null, null, 27.0]},
    {"piece": "lower", "group_tags": {}, "values": [null, null, 22.6]}
  ],
  "statistics": {
    "algorithm": "linear",
    "deviations": 2.0,
    "slope": 0.15,
    "intercept": 23.0,
    "r_squared": 0.89,
    "historical_points": 2,
    "forecast_points": 1,
    "series_count": 1,
    "execution_time_ms": 28.5
  }
}
```

### Declined groups

`forecast()` and `anomalies()` fan out over every series their sub-query
resolves to, and a group with too few observations is **declined**: it is
absent from `series` entirely rather than being answered with a fabricated
number. A group is declined when it carries

- fewer than `minDataPoints` (default 10) **finite** observations — counted over
  the group's own samples rather than over the shared time axis, so a device
  that stopped reporting is not credited with the other devices' timestamps.
  For `anomalies()` this applies only to a group **longer than**
  `minDataPoints`; a shorter group is always answered whatever it holds,
  because its stored values are returned under `raw` and `predictions` and only
  the envelope is empty (see [docs/anomaly-detection.md](anomaly-detection.md),
  "Minimum Data"). For `forecast()` it applies to any group, however short: a
  forecast under `minDataPoints` produced no values to lose. The two therefore
  disagree about a short group; that disagreement predates the fan-out work and
  is preserved deliberately; or
- fewer than **three** usable points once the chosen linear model has selected
  its data (`model='simple'` fits only the last half of the input) — a
  two-point fit has zero residual degrees of freedom, so its confidence band
  would be exactly zero wide, which reads as certainty the model does not have.

Two counters describe the outcome, in both the JSON and the protobuf encoding:

| Field | Meaning |
|-------|---------|
| `series_count` | groups actually **emitted** under `series` (forecast only) |
| `declined_series_count` | groups the query resolved to but declined |

Their sum is the number of groups the sub-query resolved to. A declined group
is not a failed read — nothing is wrong with the stored data, there is just too
little of it — so the response is still `"status": "success"`; this is
deliberately not the `QUERY_INCOMPLETE` case.

`declined_series_count` is **additive**: it is omitted from the JSON when it is
zero, and (as a proto3 scalar) is likewise absent from the protobuf wire
format. Absent means none.

### Fan-out limits

Three bounds guard a leg, all reported as HTTP 400 with the numbers that tripped
them:

| Bound | Default | What it counts | Applies to |
|-------|---------|----------------|------------|
| per-leg series | 200 | series a sub-query resolves to, one per (tag set × numeric field) | fan-out legs |
| result points | 250,000 | cells — series × the length of the shared time axis | fan-out legs (2+ series) |
| forecast output points | 500,000 | series × (historical points + forecast horizon) | **every** `forecast()` leg |

The cell bound is the one that binds on any fan-out leg large enough to be
expensive: 200 series reach it at a 1250-slot axis. The series bound is a
cardinality sanity check for unscoped legs over high-cardinality measurements;
note that the series count is (tag set × field), so a two-metric per-device query
(`avg:m(v,w){} by {dev}`) over a 50-device fleet counts as 100.

The forecast output bound exists because the forecast horizon is
`duration / interval` — the requested window measured in the data's own sampling
interval — which is unrelated to the size of the input when the window is much
wider than the data's span. It applies to a **single-series** leg as well, unlike
the cell bound, which explicitly **exempts** a lone series to keep returning data
the user actually stored — forecast slots are not stored data. (The series bound
has no exemption and needs none: a lone series is simply under the limit.) See
[docs/forecasting.md](forecasting.md).

Its budget is deliberately **twice** the cell bound's rather than equal to it.
For a leg that spans its window the horizon is about the axis length, so the
quantity it counts is roughly twice the cell count; at equal constants it would
be strictly tighter on every forecast and would refuse shapes the cell bound was
calibrated to admit — including a 20-device × 10-second-bucket daily dashboard.
At 2× the two sit in their intended relationship: the cell bound governs the
input matrix (and is the only one that binds on `anomalies()`, which has no
horizon), and the forecast bound binds only when the horizon is out of
proportion to the data.

The three messages are distinguishable: the series bound names the per-leg
series limit, the cell bound says a sub-query "fans out to N series over a shared
time axis", and the forecast bound says a forecast "would produce N points".

### Error envelope

**Error (400/500):**
```json
{
  "status": "error",
  "error_code": "QUERY_ERROR",
  "message": "Error description",
  "error": "Error description"
}
```

`error_code` is `QUERY_ERROR` for a `400` and `INTERNAL_ERROR` for a `500`.
`error` and `message` carry the same human-readable string; `error` is the field
to assert on, `message` mirrors it for backwards compatibility. Messages are
capped at 4 KB.

**Error bodies are the same on every endpoint.** This is the canonical shape
`timestar::http::jsonError` emits for all HTTP handlers, so a `/query` failure
is byte-shaped identically — `error` is a plain **string** there too:

```json
{"status":"error","error_code":"INVALID_QUERY",
 "message":"Query parse error: Query needs to specify an aggregation method",
 "error":"Query parse error: Query needs to specify an aggregation method"}
```

A client can therefore use one error accessor across `/query` and `/derived`.

**Known inconsistency — documented, not changed.** It is confined to the
**success** side, where `/derived` carries a vestigial `error` member that
`/query` does not have at all:

| Response | `error` |
|---|---|
| `/derived` **success**, arithmetic formula | present: an always-empty object `{"code":"","message":""}` |
| `/derived` **success**, `forecast()`/`anomalies()` | present: an always-empty object `{"message":""}` — no `code` member |
| `/query` **success** | **absent** — the body is `status` + `series` + `statistics` |
| `/derived` and `/query` **error** | a **string**, alongside `error_code` and `message` |

So a `/derived` client cannot use the *presence* of `error`, or
`typeof body.error`, to decide whether the request failed — a successful
`/derived` response has an `error` key too, it is just empty and of a different
type. **Branch on `status`** (`"success"` / `"error"`), which is consistent on
both endpoints and both outcomes.

The vestigial success-side object is left alone deliberately: it is what
existing clients already receive, and removing it would break them to fix a
cosmetic wart. The protobuf encoding has no such ambiguity — an error is
`formatDerivedQueryError(code, message)`, carrying the code and message as
distinct fields.

## Limits

| Limit | Default |
|-------|---------|
| Max body size | 1 MB |

## Known limitations

Recorded rather than fixed. None of these is introduced by the fan-out work
unless stated.

- **Reactor stall on large responses (pre-existing).** A `/derived` response in
  the tens of megabytes stalls the reactor for ~65–69 ms. Symbolized, the time
  is in `glz::write_json<GlazeForecastResponse>` on the serializing shard and a
  `basic_sstring` copy of the finished body on another — i.e. **JSON
  serialization and the body copy into the reply**, not the fit and not the
  fan-out. It reproduces on the pristine pre-campaign binary at 69 ms for the
  same 37.57 MB body, and only on a cold heap. This project treats reactor
  stalls as defects, so it is a real one; the fix is chunked or yielding
  serialization, not a smaller constant. Lowering
  `maxForecastOutputPoints` to hide it would refuse dashboards the bound was
  raised to admit.
- **`anomalies()` has no emitted-group counter.** `AnomalyStatistics` carries
  `declined_series_count` but nothing corresponding to forecast's
  `series_count`, so the invariant *emitted + declined = resolved* has no
  anomaly analogue. Count `series` entries and divide by the piece count if you
  need it.
- **Top-level forecast statistics describe only the first group.** Under
  fan-out, `slope`, `intercept`, `r_squared` and `residual_std_dev` are those of
  the **first emitted group** (first in the [fan-out order](#ordering)), and
  nothing in the response says which group that is. Verified live: a leg whose
  two groups have slopes of +1 and −500 answers `series_count: 2` with a single
  `"slope"` equal to whichever group sorts first. Treat them as meaningful only
  for a single-group answer.
- **`ForecastExecutor::executeMulti` auto-windows from the first series only.**
  The history trim is sized from `seriesValues[0]` and applied to every group.
  Unreachable from `/derived`, which never sets `forecastSeasonality`, but it is
  a latent asymmetry for any future caller that does.
- **`ForecastExecutor::generateForecastTimestamps` can wrap `uint64`** for a
  window ending near 2^64. Pre-existing; the new output bound narrows the
  reachable range but does not close it.
- **One arm of the field-broadcast rule cannot succeed.** The
  `matched == outputKeys.size()` ("the field is named by **all** output keys")
  branch of `fieldBroadcastsOverEveryGroup` can never produce a `200`: whenever
  the field is part of the pairing key at all, the output keys span more than one
  field, so "all" is unreachable. It is kept and tested because it is still
  observable — it decides **which** sub-query a `400` blames, and removing it
  would move the blame to an innocent leg.
