# Benchmark report

Performance and memory measurements of `Connection`, taken with the suite in
[`tests/bench`](../tests/bench). Every number here is client overhead plus
loopback against an in-process `httptest` server — never the network — so read
them as relative costs, not as the latency a real API will show.

- **Date:** 2026-09-24
- **Machine:** Intel Core Ultra 9 275HX (24 logical cores, hybrid P/E cores),
  Windows 11 Home 10.0.26200
- **Go:** 1.26.5 windows/amd64
- **Code:** working tree before the first commit, including the async-batch
  body fix described under [Issues found](#issues-found)
- **Updated:** 2026-09-24. All five optimizations the first run pointed to are
  now in. [Optimizations](#optimizations) has their before-and-after numbers.
  The sections from [Method](#method) to
  [Memory and lifecycle](#memory-and-lifecycle) still describe the code
  *before* them, and serve as the baseline.

## Contents

- [Summary](#summary)
- [Method](#method)
- [Overhead over net/http](#overhead-over-nethttp)
- [Requests](#requests)
- [Concurrency and batches](#concurrency-and-batches)
- [Optional features](#optional-features)
- [Middleware](#middleware)
- [Reading responses](#reading-responses)
- [Compression](#compression)
- [Memory and lifecycle](#memory-and-lifecycle)
- [Optimizations](#optimizations)
- [Issues found](#issues-found)
- [Caveats](#caveats)

---

## Summary

After the optimizations:

- **Overhead over `net/http` is down to about 2% and 5 allocations** on a
  small GET, from 7–9% and 25. Every request drops 20 allocations and one
  goroutine.
- **`Bytes()` now beats plain `net/http` + `io.ReadAll`.** It is 33–66%
  faster at 64 KiB and 1 MiB, and holds 1× the body instead of 2.15×.
- **Compression is 5× faster and allocates 99% less.** A pooled writer uses
  9.8 KB per call, down from 823 KB.
- **`Discard()` keeps the connection for large bodies.** At 1 MiB it no
  longer costs a redial.

The first run, which the rest of this report describes:

- **The library costs about 3 µs, 25 allocations and 2 KB per request** over a
  bare `net/http` client. That is 7–9% on a small GET and disappears into the
  noise from 64 KiB up.
- **About half of that is one avoidable effect.** Wrapping the transport makes
  `net/http` fall back to a legacy timeout path that starts a goroutine and a
  timer per request. See [Optimizations](#optimizations).
- **Memory per pooled connection matches `net/http`:** 25.0 KB and 3
  goroutines, 0.6% above the baseline. Sustained load shows no heap growth and
  no leaked goroutines.
- **Keep-alive is worth 7×** (43 µs vs 300 µs per request), and it is off by
  default.
- **Middleware is close to free:** the first one costs 5 allocations, each
  after that 1, and no time difference is measurable up to 16 deep.
- **Stream large bodies.** `Bytes()` on 1 MiB takes 3× as long as `Save()`
  and allocates 2.26 MB, where streaming allocates 9 KB.
- **Async batches of 100 run 2.8× faster than sync**, 19.8 µs per request.

---

## Method

Most numbers are the **median of 5 runs** from a port-safe run:

```sh
go test ./tests/bench -short -run '^$' -bench . -benchmem -count 5
```

`±` is the largest deviation of any run from that median. Allocation counts are
stable to within 1% everywhere, so they are the most trustworthy column; times
on CPU-bound benchmarks move by up to ±20% (see [Caveats](#caveats)).

Three benchmarks dial a new TCP connection every iteration and are skipped by
`-short` to spare Windows' ephemeral ports: `KeepAlive/off`,
`ConnectionFootprint` and `ResponseRead/Discard/1MiB`. Their numbers come from
**one full run** without `-short`, and they are only compared with other
results from that same run. They are marked *(single run)*. Since
[optimization 5](#5-discard-drains-up-to-maxresponsebodysize),
`Discard/1MiB` no longer dials per iteration and `-short` no longer skips it.

Allocation sources were attributed with a memory profile of
`BenchmarkGet/1KiB` and `BenchmarkBaselineGet/1KiB` at `-memprofilerate 1`.

---

## Overhead over net/http

`BenchmarkGet` against `BenchmarkBaselineGet`: one sequential GET on a warm
pool, body read fully. The baseline is a plain `http.Client` with the same pool
limits and the same 30 s timeout.

| Body | jr_requester | net/http | Time | Memory | Allocs |
| --- | --- | --- | --- | --- | --- |
| 0 B | 37.4 µs ±0% | 34.5 µs ±1% | +8.7% | 9,548 B (+2,050) | 100 (+25) |
| 1 KiB | 42.8 µs ±1% | 40.0 µs ±1% | +7.1% | 11,650 B (+2,053) | 112 (+25) |
| 64 KiB | 122.7 µs ±4% | 129.0 µs ±4% | −4.9% (noise) | 150,506 B (+3,380) | 127 (+25) |
| 1 MiB | 1,077 µs ±13% | 1,168 µs ±8% | −7.8% (noise) | 2,257,120 B (+7,850) | 151 (+25) |

The overhead is a constant **25 allocations and about 3 µs** per request. It
does not grow with body size. At 64 KiB and above it is smaller than the
run-to-run spread; the negative figures there are noise, not a speed-up.

Where the 25 extra allocations come from, per request:

| Source | Allocs | Why |
| --- | --- | --- |
| `net/http.setRequestCancel`, legacy path | ~13 | The client's transport is not a bare `*http.Transport`, so `Client.Timeout` is enforced with a channel, a timer, a `sync.OnceFunc` and a goroutine. |
| `DoRequest`: `req.Clone` | 3 | The outgoing copy of the caller's request. |
| `DoRequest`: `context.WithCancel` + `release` closure | 3 | The per-request cancel that `Shutdown` fires. |
| `DoRequest`: `releaseOnClose` | 1 | Unregisters the request when the body is closed. |
| `Response` wrapper, `LimitReader`, drain on close | 3 | `newResponse`, `MaxResponseBodySize` enforcement, `io.CopyN` in `Close`. |
| Remainder | ~2 | Small differences elsewhere in the request path, not attributed individually. |

---

## Requests

### GET and POST by size

| Benchmark | Time | Throughput | Memory | Allocs |
| --- | --- | --- | --- | --- |
| GET 0 B | 37.4 µs ±0% | — | 9,548 B | 100 |
| GET 1 KiB | 42.8 µs ±1% | 23.9 MB/s | 11,650 B | 112 |
| GET 64 KiB | 122.7 µs ±4% | 534 MB/s | 150,506 B | 127 |
| GET 1 MiB | 1,077 µs ±13% | 974 MB/s | 2,257,120 B | 151 |
| POST 0 B | 37.7 µs ±2% | — | 9,569 B | 103 |
| POST 1 KiB | 38.3 µs ±1% | 26.7 MB/s | 10,114 B | 111 |
| POST 64 KiB | 73.5 µs ±1% | 892 MB/s | 44,596 B | 114 |
| POST 1 MiB | 491 µs ±2% | 2,135 MB/s | 44,267 B | 114 |

- **Request bodies are streamed, not buffered:** POST memory levels off at
  about 44 KB whether the body is 64 KiB or 1 MiB.
- **GET memory is the body itself:** these benchmarks read with `Bytes()`,
  which holds about 2.15× the body size (see
  [Reading responses](#reading-responses)).

### Keep-alive

| Benchmark | Time | Memory | Allocs |
| --- | --- | --- | --- |
| Pooled (`WithKeepAlive`), 1 KiB | 43.4 µs | 11,656 B | 112 |
| No keep-alive (the default), 1 KiB | 300.0 µs | 25,541 B | 184 |

*(single run)* Without pooling every request dials a fresh TCP connection:
**6.9× slower** and 72 more allocations, even over loopback. Over a real
network, with TLS, the gap is much larger.

### Construction

| Benchmark | Time | Memory | Allocs |
| --- | --- | --- | --- |
| `NewConnection()` + `Shutdown()`, defaults | 1.02 µs ±14% | 2,065 B | 18 |
| Same, with 6 options (keep-alive, retries, API key, headers, metrics, cookies) | 2.17 µs ±12% | 3,345 B | 27 |

Building a connection is cheap, but a new connection starts with an empty pool.
Share one connection instead of building one per task.

### Lowest level

`DoRequest` with a prebuilt request, 1 KiB, body drained by hand: **41.0 µs
±1%, 8,697 B, 102 allocs**. That is 10 allocations fewer than `Get` plus
`Bytes()`. The difference is building the request, the `Response` wrapper and
buffering the body.

---

## Concurrency and batches

### Parallel GETs

`b.RunParallel` with 24 goroutines on a pool of 96 connections:

| Body | Time per request | Throughput | Allocs |
| --- | --- | --- | --- |
| 0 B | 13.2 µs ±3% (about 76,000 req/s) | — | 99 |
| 64 KiB | 63.3 µs ±25% | 1,035 MB/s | 135 |

Allocations per request match the sequential case, so no contention shows up
in the connection's own locks. Scaling is 2.8× over sequential, not 24×,
because the in-process server competes for the same cores.

### Batches

`BatchGet` of 1 KiB responses, timed for the whole batch:

| Batch | Sync | Async | Per request, sync → async |
| --- | --- | --- | --- |
| 10 requests | 496 µs ±10% | 469 µs ±6% | 49.6 → 46.9 µs |
| 100 requests | 5,548 µs ±2% | 1,982 µs ±9% | 55.5 → **19.8 µs** |

- **Async gains little at 10 requests and 2.8× at 100.** Small batches spend
  their time on setup, not waiting.
- **Async allocates slightly more:** 1,246 KB vs 1,175 KB for 100 requests,
  from the goroutines and the body-release bookkeeping.
- **Shared body:** `DoManualBatchRequests`, one 64 KiB body POSTed to 20 URLs
  asynchronously, takes 919 µs ±6% (1,426 MB/s), 1.37 MB and 2,512 allocs per
  batch.

---

## Optional features

Each feature switched on alone, 1 KiB GET, compared with none:

| Feature | Time | vs none | Memory | Allocs |
| --- | --- | --- | --- | --- |
| none | 42.7 µs ±1% | — | 11,653 B | 112 |
| `WithMetrics` | 43.3 µs ±1% | +1.4% | 11,855 B | 116 (+4) |
| `WithRetries(3, …)`, no failures | 43.1 µs ±0% | +0.9% | 12,168 B | 115 (+3) |
| `WithApiKey` + `WithHeaders` (2) | 44.4 µs ±1% | +3.8% | 12,667 B | 124 (+12) |
| `WithCookies` | 43.2 µs ±1% | +1.2% | 11,654 B | 112 (+0) |
| `CompressionManual` | 43.0 µs ±1% | +0.5% | 11,981 B | 113 (+1) |

Everything costs under 4%. Headers and the API key cost the most, because the
request decorator clones the request again and sets each header on it.

---

## Middleware

### Stack depth

Pass-through middleware, 0 B GET:

| Depth | Memory | Allocs | Allocs added |
| --- | --- | --- | --- |
| 0 | 8,986 B | 98 | — |
| 1 | 9,553 B | 103 | +5 |
| 4 | 9,628 B | 106 | +1 each |
| 16 | 9,929 B | 118 | +1 each |

The first middleware costs 5 allocations: the stack clones the request once so
every middleware can edit it. Each one after that costs a single closure,
about 25 B.

The time column is left out on purpose. In this run depth 0 and 1 measured
51 µs while depth 4 and 16 measured 40 µs, which is an ordering effect: they
run first after the lifecycle benchmarks. An identical configuration,
`MiddlewareRecipes/none`, measured 40.1 µs ±1%, and an earlier single run had
all four depths between 39.4 and 40.1 µs. **No time cost is measurable up to 16
middleware.**

### Built-in recipes

0 B GET:

| Stack | Time | vs none | Memory | Allocs |
| --- | --- | --- | --- | --- |
| none | 40.1 µs ±1% | — | 10,137 B | 106 |
| guards: allowed hosts, required header, required query value | 41.2 µs ±1% | +2.6% | 11,565 B | 118 |
| decorators: set headers, bearer token, request id, timeout | 43.7 µs ±4% | +8.9% | 12,405 B | 134 |
| logger (discard handler) | 38.8 µs ±7% | noise | 11,376 B | 117 |

The decorators cost the most. `MiddlewareTimeout` creates a timer context and
a body wrapper, and `MiddlewareRequestID` reads `crypto/rand` and hex-encodes
the result on every request.

### Editing a live stack

| Existing entries | `Add` + `Remove` | Memory | Allocs |
| --- | --- | --- | --- |
| 0 | 144 ns ±5% | 96 B | 4 |
| 16 | 761 ns ±10% | 2,274 B | 5 |

Copy-on-write makes each edit linear in the stack size. That is irrelevant
at realistic sizes, and it is what lets requests in flight keep their
snapshot.

---

## Reading responses

The same GET, consumed four ways:

| Body | `Bytes()` | `Stream()` | `Save(io.Discard)` | `Discard()` |
| --- | --- | --- | --- | --- |
| 1 KiB | 42.6 µs, 11,656 B | 41.7 µs, 9,302 B | 41.9 µs, 9,302 B | 42.2 µs, 9,326 B |
| 64 KiB | 109.9 µs, 150,475 B | 62.1 µs, 9,304 B | 62.1 µs, 9,307 B | 62.4 µs, 9,331 B |
| 1 MiB | 1,074 µs, 2,257,210 B | 413 µs ±20%, 9,310 B | 362 µs ±9%, 9,298 B | see below |

- **`Bytes()` costs 1.8× the time at 64 KiB and 2.6–3× at 1 MiB** compared
  with streaming, and holds about 2.15× the body in memory while `io.ReadAll`
  grows its buffer. Streaming stays at a flat 9.3 KB.
- **`Discard()` past 64 KiB throws the connection away.** It drains at most
  64 KiB before closing, so a larger unread body closes the connection. From
  the single full run, 1 MiB:

  | | Time | Memory | Allocs |
  | --- | --- | --- | --- |
  | `Save(io.Discard)` | 426 µs | 9,298 B | 106 |
  | `Discard()` | 501 µs (+17%) | 23,242 B | 181 (+75) |

  The extra 75 allocations are the redial on the next request, the same
  signature as keep-alive being off.

### JSON

`Response.JSON` into a slice of structs:

| Document | Time | Throughput | Memory | Allocs |
| --- | --- | --- | --- | --- |
| 10 items (603 B) | 51.6 µs ±1% | 11.7 MB/s | 13,137 B | 148 |
| 1,000 items (62 KiB) | 752 µs ±2% | 84.5 MB/s | 328,690 B | 3,147 |

At 1,000 items about 640 µs of the 752 µs is `encoding/json`, not the
library: fetching a body of that size takes about 110 µs.

---

## Compression

The handler's helpers on 16,571 bytes of repetitive JSON-like text:

| Operation | Time | Throughput | Memory | Allocs | Ratio |
| --- | --- | --- | --- | --- | --- |
| `Compress` gzip | 274 µs ±13% | 60.5 MB/s | 823,376 B | 19 | 101× |
| `Compress` zlib | 264 µs ±16% | 62.7 MB/s | 823,334 B | 21 | 109× |
| `Decompress` gzip | 26.7 µs ±18% | 621 MB/s | 81,040 B | 19 | — |
| `Decompress` zlib | 28.8 µs ±8% | 576 MB/s | 80,420 B | 20 | — |

**Each `Compress` call allocates about 823 KB, 50× its input.** That is
`compress/flate` building a new writer's state on every call. For inputs this
small, that setup takes most of the time too.

---

## Memory and lifecycle

### Footprint per pooled connection

*(single run)* 25 connections, each holding one idle keep-alive connection,
measured after two GCs. The server's half of each TCP connection lives in the
same process and is counted in both columns.

| | Heap per connection | Goroutines per connection |
| --- | --- | --- |
| jr_requester | 25,003 B | 3 |
| net/http | 24,852 B | 3 |

The library adds **151 B (0.6%)** per connection. The 3 goroutines are
`net/http`'s own: read and write loops on the client and one server
goroutine.

### Sustained load

64-request async bursts, back to back, for about 540 bursts (≈35,000
requests) per run:

| Metric | Result |
| --- | --- |
| Time per burst | 2.15 ms ±6% (33.5 µs per request) |
| Live heap growth over the run | 22.6 KiB median, +4 to +27 KiB across the 5 runs. Two earlier single runs measured −14 and −9 KiB. |
| Requests still in flight afterwards | 0 in every run |

**No leak.** After about 35,000 requests the heap moves by tens of KiB in
either direction, with no trend. A leak of even one small allocation per
request would show as megabytes. `TestNoGoroutineLeak` also passes: a connection that served
160 requests, half of them closed unread, returns to the baseline goroutine
count after `Shutdown`.

### Shutdown

| Requests in flight | `Shutdown()` alone | Whole cycle (build, open, shut down) |
| --- | --- | --- |
| 0 | 89 ns ±22% | 924 ns |
| 32 | 7.1 µs ±24% | 94.6 µs |

Cancelling costs about **220 ns per request in flight**. Even with thousands
of requests open, the kill switch returns in well under a millisecond.

---

## Optimizations

The first run pointed to five changes. All five are now in, and each one is
covered by a test. The results come from a second `-short -count 5` run.

**How to read the times.** The machine was about 30% slower during the second
run: untouched `net/http` code (`BaselineGet` at 0 B and 1 KiB) measured
31–39% slower than in the first run. So a time is only compared with another
time **from the same run**. Allocation counts do not depend on the machine
state, so they are compared across runs directly.

### Overhead over net/http, after

Second run, library against the bare `net/http` client:

| Body | jr_requester | net/http | Time | Memory | Allocs |
| --- | --- | --- | --- | --- | --- |
| 0 B | 49.0 µs ±1% | 48.0 µs ±1% | +2.1% | 7,931 B (+437) | 80 (+5) |
| 1 KiB | 52.5 µs ±1% | 52.5 µs ±3% | ±0% | 8,858 B (−721) | 89 (+2) |
| 64 KiB | 93.6 µs ±6% | 138.9 µs ±4% | **−33%** | 74,627 B (−72,422) | 90 (−12) |
| 1 MiB | 562 µs ±9% | 1,140 µs ±6% | **−51%** | 1,067,330 B (−1,181,930) | 100 (−26) |

The first run measured +8.7% / +25 allocations at 0 B and +7.1% / +25 at
1 KiB. A separate rerun of just these benchmarks confirmed the result: +2.3%
and +2.1% on small bodies, and −42% and −66% at 64 KiB and 1 MiB.

### 1. Timeout on the request context, not `http.Client.Timeout`

`DoRequest` now builds its per-request context with `context.WithTimeout`,
and the client has no `Timeout`. `net/http` only takes its legacy cancel path
when the client has a timeout, so that path is gone, along with its channel,
timer, `sync.OnceFunc` and goroutine per request. The context stays live until
the body is closed, so the body read is still inside the budget.

| Measure (1 KiB `Stream`, no other change on this path) | Before | After |
| --- | --- | --- |
| Allocs per request | 106 | 86 (**−20**) |
| Memory per request | 9,302 B | 7,716 B (−1,586) |
| Goroutines started per request, beyond `net/http`'s own | 1 | 0 |

Every benchmark that sends a request shows the same 20 allocations less. It
was bigger than the ~13 the profile attributed to `setRequestCancel` in the
first run.

Behaviour changes:

- **Timeout errors change type.** Expiry now satisfies
  `errors.Is(err, context.DeadlineExceeded)`, and the message says
  `context deadline exceeded` instead of `Client.Timeout exceeded`.
  `err.Timeout()` is still true, and metrics still count it as a timeout.
- **`GetClient().Timeout` is 0.** Code that sends requests through the raw
  client directly gets no timeout. The doc comment and the README say so.

Tests: `TestZeroTimeoutFallsBackToDefault` now checks the deadline on the
request context. `TestTimeoutCoversHeadersAndBody` checks the timeout fires
both while waiting for headers and while reading a stalled body.

### 2. Pooled compression codecs

`Compress` and `Decompress` take gzip and zlib writers and readers from
`sync.Pool`s and `Reset` them for each call. Writers are pointed back at
`io.Discard` before going back, so they don't keep the caller's output alive.

| Benchmark | Time, before → after | Memory, before → after | Allocs |
| --- | --- | --- | --- |
| `Compress` gzip | 274 → 49.5 µs (**−82%**) | 823,376 → 9,762 B (−98.8%) | 19 → 2 |
| `Compress` zlib | 264 → 53.2 µs (**−80%**) | 823,334 → 9,635 B (−98.8%) | 21 → 2 |
| `Decompress` gzip | 26.7 → 17.8 µs (−33%) | 81,040 → 39,918 B (−51%) | 19 → 14 |
| `Decompress` zlib | 28.8 → 19.3 µs (−33%) | 80,420 → 39,912 B (−50%) | 20 → 15 |

These times do cross runs. They improved despite the slower second run, so
the real gain is if anything larger. What `Decompress` still allocates is
mostly `io.ReadAll` growing the output buffer.

Test: `TestPooledCodecsStayCorrect` runs round trips from 16 goroutines, with
malformed input mixed in, so a codec returned to the pool in a bad state would
corrupt a later call.

### 3. `Bytes()` sized from `Content-Length`

When the server declares a length within `MaxResponseBodySize`, `Bytes()`
reads into a buffer of exactly that size. Otherwise it falls back to
`io.ReadAll`. A body that falls short of its declared length is still reported
as `io.ErrUnexpectedEOF`.

| Benchmark | Memory, before → after | Allocs, before → after |
| --- | --- | --- |
| `Bytes` 64 KiB | 150,475 → 74,645 B (−50%) | 127 → 90 |
| `Bytes` 1 MiB | 2,257,210 → 1,067,340 B (−53%) | 151 → 101 |

At 1 MiB, `Bytes()` now takes 1.7× as long as `Save()` in the same run
(548 vs 331 µs), down from 3× (1,074 vs 362 µs). Memory is the body plus
about 19 KB, not 2.15× the body.

Test: `TestBytesWithDeclaredLength` covers the exact body and the truncated
case.

### 4. One request clone instead of up to three

`DoRequest` marks its request context when a middleware stack or the header
decorator is present. Both layers skip their own clone for a marked request,
since nobody else holds it. Redirect hops inherit the mark safely: `net/http`
gives each hop a fresh `Header` map, and its value slices are capped at full
capacity. A request without the mark, such as one sent through `GetClient()`,
is still cloned.

| Cost over no layers | Before | After |
| --- | --- | --- |
| First middleware | +5 allocs, +567 B | +3 allocs, +106 B |
| `WithRetries` (decorator, no headers) | +3 allocs, +515 B | +1 alloc, +65 B |
| `WithApiKey` + `WithHeaders` | +12 allocs, +1,014 B | +10 allocs, +557 B |

Test: `TestLayersDoNotLeakIntoCallerRequest` sends a request through a
middleware, cached headers and an API key. It checks that all of them reach
the server and none reach the caller's `*http.Request`.

### 5. `Discard()` drains up to `MaxResponseBodySize`

`Discard()` used to be `Close()`, which drains at most 64 KiB. It now drains
up to `MaxResponseBodySize` (everything when that is unlimited), so a large
unread body still returns its connection. A body declared larger than the
limit is closed without being read, since draining it would cost more than a
new dial. `Close()` keeps its 64 KiB cap.

| `Discard()`, 1 MiB | Before (single run) | After (focused rerun) |
| --- | --- | --- |
| Time vs `Save()` in the same run | +17% | −8% (329 vs 359 µs) |
| Allocs | 181 (a redial per request) | 87, the same as `Save()` |

`ResponseRead/Discard/1MiB` no longer dials per iteration, so `-short` no
longer skips it. Tests: `TestDiscardReturnsLargeBodyConnection` (one TCP
connection for three 1 MiB discards) and `TestDiscardSkipsBodyOverLimit`.

### Knock-on effects

Other benchmarks improved as a side effect, all measured in the second run:

| Benchmark | Allocs, before → after | Memory, before → after |
| --- | --- | --- |
| `BatchGet` async, 100 requests | 11,551 → 9,156 (−21%) | 1,246 → 950 KB (−24%) |
| `SustainedLoad`, one 64-request burst | 7,850 → 5,931 (−24%) | 1,378 → 835 KB (−39%) |
| `GetParallel` 64 KiB | 135 → 93 (−31%) | 156 → 78 KB (−50%) |
| `Shutdown` with 32 open, whole cycle | 1,504 → 830 (−45%) | 134 → 71 KB (−47%) |

---

## Issues found

Writing the benchmarks turned up two correctness problems.

- **Fixed: async batch bodies were unreadable.** `batchAsync` cancelled its
  context on return, and response bodies are read under that context. So with
  `WithAsync`, every body the transport had not already buffered failed with
  `context canceled`. The context is now released after the last body closes.
  `TestAsyncBatchBodiesReadable` guards it.
- **Documented, not fixed: batches larger than `MaxConnsPerHost` stall.** A
  batch holds every response open until it returns, so the requests beyond
  the limit wait for a connection that is never freed, until `Timeout`. The
  batch benchmarks size their pools to the batch. The README lists it under
  Gotchas.

---

## Caveats

- **Loopback only.** No real network, no TLS. Real latency makes the
  library's constant 3 µs even less significant, and it makes pooling matter
  far more.
- **Shared CPU.** The `httptest` server runs in the same process on the same
  cores, which limits parallel scaling and adds its costs to the memory
  figures.
- **Hybrid CPU.** The 275HX mixes performance and efficiency cores. CPU-bound
  benchmarks (compression, 1 MiB reads, parallel 64 KiB) vary by up to ±25%
  depending on which cores the scheduler picks. Allocation counts do not vary.
- **Order effects.** Benchmarks that run right after the lifecycle benchmarks
  can read slow; see [Stack depth](#stack-depth). Compare with benchstat
  over `-count 10` before trusting any time difference under about 10%.
- **Single-run figures** (keep-alive off, footprint, `Discard` at 1 MiB) have
  no spread estimate. They are compared only with results from the same run.

To reproduce, or to compare a change:

```sh
go test ./tests/bench -short -run '^$' -bench . -benchmem -count 10 > old.txt
# make the change
go test ./tests/bench -short -run '^$' -bench . -benchmem -count 10 > new.txt
benchstat old.txt new.txt   # go install golang.org/x/perf/cmd/benchstat@latest
```
