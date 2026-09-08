import { useState, useEffect, useRef, useMemo } from 'react';
import RadiationMap from './components/RadiationMap';
import BasemapPicker from './components/BasemapPicker';
import ControlPanel from './components/ControlPanel';
import Timeline from './components/Timeline';
import { BASEMAPS, DEFAULT_BASEMAP } from './components/basemaps';
import { msToLocalInput } from './lib/time';

const BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

// Per-client view state (REMAP model): filters live in THIS browser only —
// localStorage for persistence, URL params for shareable views. They are never
// written to the backend; POST /config is reserved for the admin pipeline form.
const VIEW_STORAGE_KEY = 'radiation-tracker.view.v1';

const DEFAULT_VIEW = {
  area: null,
  timespan: { start: '', end: '' },
  display: { warn: '', danger: '' },
};

// Cap on distinct event timestamps retained for the timeline histogram.
const SEEN_TIMES_CAP = 5000;

const BASEMAP_IDS = new Set(BASEMAPS.map((b) => b.id));

// Old builds persisted 'dark'/'light'; map them onto the new catalog ids.
const normaliseBasemap = (value) => {
  if (value === 'dark') return 'darkmatter';
  if (value === 'light') return 'positron';
  return BASEMAP_IDS.has(value) ? value : DEFAULT_BASEMAP;
};

const loadStoredView = () => {
  try {
    const raw = window.localStorage.getItem(VIEW_STORAGE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
};

// URL params beat localStorage so a shared link reproduces the sender's view.
const loadViewFromUrl = () => {
  try {
    const p = new URLSearchParams(window.location.search);
    const areaKeys = ['min_lat', 'max_lat', 'min_lon', 'max_lon'];
    const area = areaKeys.every(k => p.has(k) && !Number.isNaN(Number(p.get(k))))
      ? Object.fromEntries(areaKeys.map(k => [k, Number(p.get(k))]))
      : null;
    const timespan = { start: p.get('start') || '', end: p.get('end') || '' };
    if (!area && !timespan.start && !timespan.end) return null;
    return { area, timespan };
  } catch {
    return null;
  }
};

const initialView = () => {
  const stored = loadStoredView();
  const fromUrl = loadViewFromUrl();
  return {
    ...DEFAULT_VIEW,
    ...(stored?.view ?? {}),
    ...(fromUrl?.area ? { area: fromUrl.area } : {}),
    ...(fromUrl?.timespan?.start || fromUrl?.timespan?.end ? { timespan: fromUrl.timespan } : {}),
  };
};

const App = () => {
  const [markers, setMarkers] = useState(new Map());
  const [blobs, setBlobs] = useState([]);
  const [alerts, setAlerts] = useState([]);

  // Per-client view filters — local to this browser, never POSTed.
  const [view, setView] = useState(initialView);
  const [layers, setLayers] = useState(() => loadStoredView()?.layers ?? {
    markers: true,
    blobs: true,
    alerts: true,
  });
  const [refreshCadence, setRefreshCadence] = useState(() => loadStoredView()?.refreshCadence ?? 1000);
  const [basemap, setBasemap] = useState(() => normaliseBasemap(loadStoredView()?.basemap));
  const [panelOpen, setPanelOpen] = useState(true);
  const [wsStatus, setWsStatus] = useState('connecting');

  // Marker the map flies to when an Alert Log entry is clicked. `key` is a nonce
  // so re-clicking the same alert re-triggers the animation.
  const [focusTarget, setFocusTarget] = useState(null);

  // Admin pipeline config — global thresholds, the only server write left.
  // Seeded with the pipeline's compiled-in defaults (WARN 100 / DANGER 1000)
  // so the form and legend are meaningful even before GET /config answers
  // (or when the backend is unreachable); the fetch below overwrites them
  // with the active values.
  const [thresholds, setThresholds] = useState({ warn: 100, danger: 1000 });
  const [configError, setConfigError] = useState('');
  const [configSaved, setConfigSaved] = useState(false);

  const bufferRef = useRef({ markers: new Map(), blobs: new Map(), alerts: new Map() });
  const wsRef = useRef(null);
  const reconnectTimerRef = useRef(null);
  // Ring of the newest clean-event timestamps (epoch ms) the client has received,
  // collected raw (pre-filter) so the timeline domain stays stable when a timespan
  // filter is active. Snapshotted into `timelineSamples` state on each flush so the
  // timeline re-renders without reading the ref during render.
  const seenTimesRef = useRef([]);
  const [timelineSamples, setTimelineSamples] = useState([]);

  // Server-side half of the per-client view (ADR-017): the backend filters this
  // socket's live stream by the same area/timespan, so filtered-out events never
  // cross the wire. Client-side filtering below stays as the authoritative pass
  // (it also covers the unfiltered catch-up snapshot).
  const subscribeMessage = useMemo(() => JSON.stringify({
    type: 'subscribe',
    area: view.area ?? null,
    timespan: view.timespan.start && view.timespan.end
      ? {
          start: new Date(view.timespan.start).toISOString(),
          end: new Date(view.timespan.end).toISOString(),
        }
      : null,
  }), [view.area, view.timespan]);
  const subscribeRef = useRef(subscribeMessage);

  useEffect(() => {
    subscribeRef.current = subscribeMessage;
    const ws = wsRef.current;
    if (ws && ws.readyState === 1 && typeof ws.send === 'function') {
      try {
        ws.send(subscribeMessage);
      } catch { /* socket mid-close — onopen re-sends on reconnect */ }
    }
  }, [subscribeMessage]);

  // ── Seed the ADMIN form from GET /config (global pipeline thresholds).
  // The per-client view is intentionally NOT seeded from it.
  useEffect(() => {
    const fetchConfig = async () => {
      try {
        const res = await fetch(`${BASE_URL}/config`);
        if (res.ok) {
          const cfg = await res.json();
          setThresholds({
            warn: cfg.cpm_warn_threshold ?? cfg.warn_threshold ?? 100,
            danger: cfg.cpm_danger_threshold ?? cfg.danger_threshold ?? 1000,
          });
        }
      } catch (err) {
        console.warn('Could not fetch initial config:', err);
      }
    };
    fetchConfig();
  }, []);

  // ── Persist the view locally and mirror shareable filters into the URL ──
  useEffect(() => {
    try {
      window.localStorage.setItem(
        VIEW_STORAGE_KEY,
        JSON.stringify({ view, layers, refreshCadence, basemap })
      );
    } catch { /* private mode / quota — view just won't persist */ }
    try {
      const p = new URLSearchParams();
      if (view.area) {
        Object.entries(view.area).forEach(([k, v]) => p.set(k, String(v)));
      }
      if (view.timespan.start && view.timespan.end) {
        p.set('start', view.timespan.start);
        p.set('end', view.timespan.end);
      }
      const qs = p.toString();
      window.history.replaceState(null, '', qs ? `?${qs}` : window.location.pathname);
    } catch { /* jsdom / older browsers */ }
  }, [view, layers, refreshCadence, basemap]);

  // ── WebSocket with reconnect (timer id kept in ref, cleared on cleanup) ──
  useEffect(() => {
    let isClosed = false;

    const connectWs = () => {
      const wsUrl = BASE_URL.replace(/^http/, 'ws') + '/ws/stream';
      const ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        if (isClosed) return;
        setWsStatus('live');
        // (Re)install this client's view filter — covers first connect and
        // every reconnect, where the server starts with a fresh subscriber.
        if (typeof ws.send === 'function') {
          try {
            ws.send(subscribeRef.current);
          } catch { /* connection already gone; reconnect will retry */ }
        }
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          const newEvents = Array.isArray(data) ? data : [data];

          newEvents.forEach(msg => {
            if (msg.type === 'clean') {
              bufferRef.current.markers.set(msg.data.sensor_id, msg.data);
              // Record the event time for the timeline domain (raw, pre-filter).
              const t = Date.parse(msg.data.captured_at);
              if (Number.isFinite(t)) {
                const ring = seenTimesRef.current;
                ring.push(t);
                if (ring.length > SEEN_TIMES_CAP) ring.splice(0, ring.length - SEEN_TIMES_CAP);
              }
            } else if (msg.type === 'aggregated') {
              bufferRef.current.blobs.set(`${msg.data.geohash}_${msg.data.window_start}`, msg.data);
            } else if (msg.type === 'alert') {
              bufferRef.current.alerts.set(`${msg.data.sensor_id}_${msg.data.window_start}`, msg.data);
            }
          });
        } catch (err) {
          console.error('WS Parse error', err);
        }
      };

      ws.onclose = () => {
        if (!isClosed) {
          console.log('WS closed, reconnecting in 5s...');
          setWsStatus('reconnecting');
          // Store timer id so cleanup can cancel it (no post-unmount socket)
          reconnectTimerRef.current = setTimeout(connectWs, 5000);
        }
      };

      ws.onerror = (err) => {
        console.error('WS error', err);
      };

      wsRef.current = ws;
    };

    connectWs();

    return () => {
      isClosed = true;
      // Cancel any pending reconnect so we never create a post-unmount socket
      if (reconnectTimerRef.current !== null) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      if (wsRef.current) wsRef.current.close();
    };
  }, []);

  // ── Flush buffer → state at configured cadence ──
  // markers state is a Map keyed on sensor_id → O(1) lookup + bounded memory.
  // Each buffer is snapshotted and cleared BEFORE setState: React may run a
  // functional updater lazily (at render time), so an updater that read
  // bufferRef directly would see an already-cleared buffer whenever another
  // state update is pending, silently dropping the batch.
  useEffect(() => {
    const flushInterval = setInterval(() => {
      if (bufferRef.current.markers.size > 0) {
        const pendingMarkers = new Map(bufferRef.current.markers);
        bufferRef.current.markers.clear();
        setMarkers(prev => {
          // Merge buffered updates into the existing Map (O(1) per update, memory-bounded)
          const next = new Map(prev);
          pendingMarkers.forEach((data, key) => {
            // Cap at 2000 unique sensors to prevent unbounded memory on long soak
            if (!next.has(key) && next.size >= 2000) return;
            next.set(key, data);
          });
          return next;
        });
        // New event times arrived → snapshot the seen ring for the timeline.
        setTimelineSamples(seenTimesRef.current.slice());
      }

      if (bufferRef.current.blobs.size > 0) {
        const pendingBlobs = new Map(bufferRef.current.blobs);
        bufferRef.current.blobs.clear();
        setBlobs(prev => {
          const next = [...prev];
          pendingBlobs.forEach((data) => {
            const idx = next.findIndex(b => b.geohash === data.geohash && b.window_start === data.window_start);
            if (idx !== -1) next[idx] = data;
            else next.push(data);
          });
          return next.slice(-100);
        });
      }

      if (bufferRef.current.alerts.size > 0) {
        const pendingAlerts = new Map(bufferRef.current.alerts);
        bufferRef.current.alerts.clear();
        setAlerts(prev => {
          const next = [...prev];
          pendingAlerts.forEach((data) => {
            const idx = next.findIndex(a => a.sensor_id === data.sensor_id && a.window_start === data.window_start);
            if (idx !== -1) next[idx] = data;
            else next.push(data);
          });
          return next.slice(-100);
        });
      }
    }, refreshCadence);

    return () => clearInterval(flushInterval);
  }, [refreshCadence]);

  // ── Admin: POST global thresholds to /config (token-gated on the droplet).
  // View filters (area/timespan/display colors) are NOT part of this payload.
  const handleConfigUpdate = async (e) => {
    e.preventDefault();
    setConfigError('');
    setConfigSaved(false);

    const warnNum = Number(thresholds.warn);
    const dangerNum = Number(thresholds.danger);

    // Client-side guard: reject empty or non-positive threshold values
    if (!thresholds.warn || !thresholds.danger || isNaN(warnNum) || isNaN(dangerNum)) {
      setConfigError('Both warn and danger thresholds are required.');
      return;
    }
    if (warnNum <= 0 || dangerNum <= 0) {
      setConfigError('Thresholds must be positive numbers.');
      return;
    }
    if (warnNum >= dangerNum) {
      setConfigError('Warn threshold must be less than danger threshold.');
      return;
    }

    try {
      const headers = { 'Content-Type': 'application/json' };
      // Send the shared token header when set (ADR-013 / droplet gate — blank in local dev)
      const writeToken = import.meta.env.VITE_CONFIG_WRITE_TOKEN || '';
      if (writeToken) headers['X-Config-Token'] = writeToken;

      const response = await fetch(`${BASE_URL}/config`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          cpm_warn_threshold: warnNum,
          cpm_danger_threshold: dangerNum,
        })
      });

      if (response.status === 401 || response.status === 403) {
        setConfigError('Config update rejected: invalid or missing token (401/403).');
      } else if (response.status === 422) {
        let detail = 'Validation error (422) — check that warn < danger.';
        try {
          const body = await response.json();
          if (body.detail) detail = `Validation error: ${JSON.stringify(body.detail)}`;
        } catch { /* ignore parse failure */ }
        setConfigError(detail);
      } else if (!response.ok) {
        setConfigError(`Config update failed (${response.status}). Check backend connectivity.`);
      } else {
        setConfigSaved(true);
      }
    } catch (err) {
      console.error('Error updating config', err);
      setConfigError('Network error — backend may be unreachable.');
    }
  };

  // ── Per-client filtering, applied at render time ──
  const inArea = (lat, lon) => {
    const a = view.area;
    if (!a) return true;
    return lat >= a.min_lat && lat <= a.max_lat && lon >= a.min_lon && lon <= a.max_lon;
  };

  const inTimespan = (iso) => {
    const { start, end } = view.timespan;
    if (!start || !end) return true;
    const t = new Date(iso).getTime();
    return t >= new Date(start).getTime() && t <= new Date(end).getTime();
  };

  // Local display thresholds recolor markers in this client only; blank = use
  // the pipeline's classification as-is.
  const displayThresholds = useMemo(() => {
    const warn = Number(view.display.warn);
    const danger = Number(view.display.danger);
    const valid =
      view.display.warn !== '' && view.display.danger !== '' &&
      !Number.isNaN(warn) && !Number.isNaN(danger) && warn > 0 && warn < danger;
    return valid ? { warn, danger } : null;
  }, [view.display]);

  const visibleMarkers = Array.from(markers.values()).filter(
    item => inArea(item.latitude, item.longitude) && inTimespan(item.captured_at)
  );
  const visibleBlobs = blobs.filter(
    item => inArea(item.centroid_latitude, item.centroid_longitude) && inTimespan(item.window_start)
  );
  const visibleAlerts = alerts.filter(
    item => inArea(item.latitude, item.longitude) && inTimespan(item.window_start || item.triggered_at)
  );

  const filtersActive = Boolean(view.area || (view.timespan.start && view.timespan.end));

  // ── Alert Logs → fly the map to the clicked alert's marker ──
  const focusOnAlert = (alert) => {
    if (alert.latitude == null || alert.longitude == null) return;
    setFocusTarget({ lat: alert.latitude, lon: alert.longitude, key: Date.now() });
  };

  // ── Timeline (timelineSamples is refreshed from the seen ring on each flush) ──
  const timelineSelection = useMemo(() => {
    const { start, end } = view.timespan;
    if (!start || !end) return null;
    const s = new Date(start).getTime();
    const e = new Date(end).getTime();
    return Number.isFinite(s) && Number.isFinite(e) ? { start: s, end: e } : null;
  }, [view.timespan]);

  const onTimelineSelect = (aMs, bMs) => {
    setView(v => ({ ...v, timespan: { start: msToLocalInput(aMs), end: msToLocalInput(bMs) } }));
  };
  const onTimelineClear = () => {
    setView(v => ({ ...v, timespan: { start: '', end: '' } }));
  };

  return (
    <div className="app-shell" data-basemap={basemap}>
      <header className="topbar">
        <div className="topbar-left">
          <button
            type="button"
            className="panel-toggle"
            aria-label="Toggle control panel"
            aria-expanded={panelOpen}
            onClick={() => setPanelOpen(open => !open)}
          >
            <span aria-hidden="true">☰</span>
          </button>
          <div className="brand">
            <h1>Radiation Tracker</h1>
            <span className="brand-sub">Live gamma count-rate monitoring</span>
          </div>
        </div>
        <div className="topbar-stats" aria-label="Stream statistics">
          <span className="stat-chip">
            <strong>{visibleMarkers.length}</strong> sensors in view
          </span>
          <span className={`stat-chip ${visibleAlerts.length > 0 ? 'stat-chip-alert' : ''}`}>
            <strong>{visibleAlerts.length}</strong> alerts
          </span>
          {filtersActive && <span className="stat-chip stat-chip-filter">filtered view</span>}
        </div>
        <div className="topbar-right">
          <div className={`status-indicator status-${wsStatus}`}>
            <span className="pulse-dot" aria-hidden="true"></span>
            {wsStatus === 'live' ? 'Live' : wsStatus === 'reconnecting' ? 'Reconnecting…' : 'Connecting…'}
          </div>
        </div>
      </header>

      <div className="app-body">
        <ControlPanel
          open={panelOpen}
          view={view}
          setView={setView}
          layers={layers}
          setLayers={setLayers}
          refreshCadence={refreshCadence}
          setRefreshCadence={setRefreshCadence}
          displayThresholds={displayThresholds}
          pipelineThresholds={thresholds}
          setPipelineThresholds={setThresholds}
          onPipelineSubmit={handleConfigUpdate}
          configError={configError}
          configSaved={configSaved}
          alerts={visibleAlerts}
          onFocusAlert={focusOnAlert}
          apiBase={BASE_URL}
        />
        <main className="map-area">
          <RadiationMap
            markers={visibleMarkers}
            blobs={visibleBlobs}
            alerts={visibleAlerts}
            layers={layers}
            displayThresholds={displayThresholds}
            basemap={basemap}
            panelOpen={panelOpen}
            focusTarget={focusTarget}
          />
          <BasemapPicker basemap={basemap} setBasemap={setBasemap} />
          <Timeline
            samples={timelineSamples}
            selection={timelineSelection}
            onSelect={onTimelineSelect}
            onClear={onTimelineClear}
          />
        </main>
      </div>
    </div>
  );
};

export default App;
