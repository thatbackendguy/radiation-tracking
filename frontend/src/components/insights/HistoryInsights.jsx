import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import TrendChart from './TrendChart';
import HotspotBars from './HotspotBars';
import './HistoryInsights.css';

const REFRESH_MS = 15000;

const fmtSpan = (first, last) => {
  if (!first || !last) return '—';
  const f = new Date(first);
  const l = new Date(last);
  const opts = { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' };
  return `${f.toLocaleString(undefined, opts)} → ${l.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}`;
};

const StatTile = ({ label, value, tone }) => (
  <div className={`stat-tile ${tone ? `stat-tile-${tone}` : ''}`}>
    <span className="stat-value">{value}</span>
    <span className="stat-label">{label}</span>
  </div>
);

/**
 * Insights over the Database aggregated-blob archive. Reads
 * /history/{summary,timeseries,hotspots} scoped to the current per-client view
 * (area → bbox, timespan → range), so the charts follow what the user is looking
 * at. Degraded-safe: a 503 renders an "archive unavailable" note.
 */
const HistoryInsights = ({ apiBase, view, thresholds, onFocus }) => {
  const [summary, setSummary] = useState(null);
  const [series, setSeries] = useState([]);
  const [hotspots, setHotspots] = useState([]);
  const [status, setStatus] = useState('loading'); // loading | ok | unavailable | error
  const abortRef = useRef(null);

  // Build the shared bbox + time-range query string from the current view.
  const query = useMemo(() => {
    const p = new URLSearchParams();
    if (view.area) Object.entries(view.area).forEach(([k, v]) => p.set(k, String(v)));
    if (view.timespan.start && view.timespan.end) {
      p.set('start', new Date(view.timespan.start).toISOString());
      p.set('end', new Date(view.timespan.end).toISOString());
    }
    return p.toString();
  }, [view.area, view.timespan]);

  const load = useCallback(async () => {
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    const qs = query ? `?${query}` : '';
    try {
      const [s, t, h] = await Promise.all([
        fetch(`${apiBase}/history/summary`, { signal: ctrl.signal }),
        fetch(`${apiBase}/history/timeseries${qs}`, { signal: ctrl.signal }),
        fetch(`${apiBase}/history/hotspots${qs}${qs ? '&' : '?'}limit=8`, { signal: ctrl.signal }),
      ]);
      if (s.status === 503) {
        setStatus('unavailable');
        return;
      }
      if (!s.ok || !t.ok || !h.ok) {
        setStatus('error');
        return;
      }
      const [sj, tj, hj] = await Promise.all([s.json(), t.json(), h.json()]);
      setSummary(sj && typeof sj === 'object' ? sj : null);
      setSeries(Array.isArray(tj) ? tj : []);
      setHotspots(Array.isArray(hj) ? hj : []);
      setStatus('ok');
    } catch (err) {
      if (err.name !== 'AbortError') setStatus('error');
    }
  }, [apiBase, query]);

  useEffect(() => {
    // Defer the initial fetch off the effect body (it sets state) and refresh on
    // an interval as the archive grows.
    const kick = setTimeout(load, 0);
    const id = setInterval(load, REFRESH_MS);
    return () => {
      clearTimeout(kick);
      clearInterval(id);
      abortRef.current?.abort();
    };
  }, [load]);

  if (status === 'unavailable') {
    return <p className="insights-empty">Archive unavailable — the Postgres history service isn&apos;t reachable.</p>;
  }
  if (status === 'error') {
    return <p className="insights-empty">Couldn&apos;t load insights. Retrying…</p>;
  }
  if (status === 'loading' && !summary) {
    return <p className="insights-empty">Loading insights…</p>;
  }

  const empty = summary && summary.rows === 0;

  return (
    <div className="insights">
      <div className="stat-grid">
        <StatTile label="windows archived" value={(summary?.rows ?? 0).toLocaleString()} />
        <StatTile label="geohash cells" value={(summary?.cells ?? 0).toLocaleString()} />
        <StatTile label="anomalies" value={(summary?.anomalies ?? 0).toLocaleString()} tone={summary?.anomalies ? 'alert' : null} />
        <StatTile label="peak CPM" value={summary?.peak_cpm != null ? Math.round(summary.peak_cpm) : '—'} />
      </div>
      <p className="insights-span">Archived span: {fmtSpan(summary?.first_window, summary?.last_window)}</p>

      {empty ? (
        <p className="insights-empty">No archived data yet — the sink populates this once aggregated blobs flow.</p>
      ) : (
        <>
          <h4 className="insights-h">CPM over time{view.area || (view.timespan.start && view.timespan.end) ? ' (your view)' : ''}</h4>
          <TrendChart points={series} />

          <h4 className="insights-h">Top hotspots by peak CPM</h4>
          <HotspotBars hotspots={hotspots} thresholds={thresholds} onFocus={onFocus} />
        </>
      )}
    </div>
  );
};

export default HistoryInsights;
