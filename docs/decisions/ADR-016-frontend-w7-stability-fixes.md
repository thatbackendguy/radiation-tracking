# ADR-016: Frontend W7 Stability Fixes — Config Seed, O(1) Markers, Reconnect Timer

**Date:** 2026-07-08
**Status:** Accepted
**Author:** M5 (Pranav Tiwari)
**Affects:** `frontend/src/App.jsx`, `frontend/src/index.css`, `frontend/.env.example`

---

## Context

During the 2026-07-03 main review, three stability issues were identified in the frontend
that matter for the droplet's 24-hour soak test. All three are M5's to fix.

### Finding (a) — hardcoded thresholds on load

`App.jsx` initialised `thresholds` state with `{ warn: 100, danger: 300 }`. The backend's
compiled-in defaults are `cpm_warn_threshold: 100.0` and `cpm_danger_threshold: 1000.0`. No `GET /config` call was made on mount,
so the form always showed a danger value that did not match Flink's active state (300 vs 1000). A user who opened
the dashboard without changing the form would see correct markers on the map but a config
panel that said something entirely different.

### Finding (b) — O(n) marker scan, unbounded array

`markers` was an unbounded `Array`. Each flush cycle called `Array.findIndex` for every
buffered sensor — O(n) per update. In a 24-hour soak with thousands of unique sensors, this
degrades the flush loop noticeably and grows memory without bound.

### Finding (c) — zombie reconnect timer + silent 422

1. `setTimeout(connectWs, 5000)` in the `ws.onclose` handler never stored the timer id.
   On unmount, `isClosed` was set to `true` (so no *new* reconnect after the first close),
   but if a close fired after the cleanup but before `isClosed` propagated, the timer could
   re-create a WebSocket after the component was gone.

2. A cleared `<input>` sends `Number('') = 0` to the backend, which returns 422. No
   user-visible error was shown — the form appeared to succeed but the backend rejected
   the update silently.

---

## Decision

### (a) Fetch `GET /config` on mount

Add a `useEffect(() => { fetchConfig(); }, [])` that calls `${BASE_URL}/config` on mount
and seeds `thresholds`, `area`, and `timespan` from the response. Failures are caught and
logged; the form falls back to blank inputs rather than showing a stale hardcoded value.

This aligns the config panel with Flink's active broadcast state from the first render.

### (b) Replace `markers` array with a `Map`

Change `const [markers, setMarkers] = useState([])` to `useState(new Map())`.
The flush loop does `next.set(sensor_id, data)` — O(1). A 2 000-entry cap (`!next.has(key)
&& next.size >= 2000`) bounds memory for long soak runs. Before passing to `<RadiationMap>`,
the Map is converted to an array via `Array.from(markers.values())`.

Blobs and alerts remain as capped arrays (already at 100 entries) — their key space is
composite (`geohash + window_start`) and they were already bounded.

### (c) Store reconnect timer id; validate before POST; surface errors

1. The `setTimeout(connectWs, 5000)` return value is stored in `reconnectTimerRef.current`
   and `clearTimeout`'d in the effect cleanup function, eliminating the zombie-socket path.

2. Before calling `fetch(POST /config)`, the handler validates:
   - Both warn and danger are non-empty, parseable numbers.
   - Both are positive (> 0).
   - `warn < danger`.
   On failure, `setConfigError(...)` sets an error string; the POST is skipped entirely.

3. For HTTP errors (401, 403, 422, 5xx), the response body is inspected for `detail` and
   the error string is set. A `.config-error` paragraph with `role="alert"` renders below
   the submit button — immediately visible, accessible via screen readers.

4. `X-Config-Token` header is sent when `VITE_CONFIG_WRITE_TOKEN` is set, satisfying the
   ADR-013 gate on the droplet without breaking local dev (where the var is blank).

---

## Consequences

- **Positive:** The config panel now always reflects the backend's active state on load.
  Memory and CPU are bounded over long-running soak tests. The reconnect path is safe after
  unmount. Users get immediate feedback on invalid or rejected config submissions.

- **Neutral:** `markers` is now a `Map` internally — the `Array.from` conversion before
  rendering is O(n) but happens only at flush time (not per-message), which is acceptable.

- **Backward compatible:** No API contract changes. The `GET /config` call requires the
  backend to be up (it gracefully degrades on failure). `VITE_CONFIG_WRITE_TOKEN` is
  optional (blank = gate off).
