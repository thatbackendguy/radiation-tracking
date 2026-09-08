import { useMemo, useRef, useState } from 'react';
import { fracToMs } from '../lib/time';
import './Timeline.css';

const BINS = 60;

const fmtLabel = (ms) =>
  new Date(ms).toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });

/**
 * Live timeline scrubber. Shows a density histogram of the event times the client
 * has received (so the user can see *which data is present*) and lets them drag a
 * brush to set the time filter. It drives the existing `view.timespan` filter via
 * onSelect/onClear — no data processing of its own beyond binning for display.
 */
const Timeline = ({ samples, selection, onSelect, onClear }) => {
  const trackRef = useRef(null);
  const [drag, setDrag] = useState(null); // { a: ms, b: ms } while dragging

  const { min, max, bins, peak } = useMemo(() => {
    if (!samples || samples.length === 0) {
      return { min: null, max: null, bins: [], peak: 1 };
    }
    let lo = Infinity;
    let hi = -Infinity;
    for (const t of samples) {
      if (!Number.isFinite(t)) continue;
      if (t < lo) lo = t;
      if (t > hi) hi = t;
    }
    if (!Number.isFinite(lo) || !Number.isFinite(hi)) {
      return { min: null, max: null, bins: [], peak: 1 };
    }
    const span = hi - lo || 1;
    const counts = new Array(BINS).fill(0);
    for (const t of samples) {
      if (!Number.isFinite(t)) continue;
      let i = Math.floor(((t - lo) / span) * BINS);
      if (i === BINS) i = BINS - 1;
      if (i >= 0 && i < BINS) counts[i] += 1;
    }
    return { min: lo, max: hi, bins: counts, peak: Math.max(1, ...counts) };
  }, [samples]);

  if (min == null || max == null || min === max) {
    return (
      <div className="timeline timeline-empty" aria-label="Data timeline">
        Waiting for data…
      </div>
    );
  }

  const span = max - min;
  const pct = (ms) => `${((ms - min) / span) * 100}%`;

  // The band to paint: the active drag, else the committed selection (clamped to domain).
  const band = drag
    ? { start: Math.min(drag.a, drag.b), end: Math.max(drag.a, drag.b) }
    : selection && selection.start != null && selection.end != null
      ? { start: Math.max(min, selection.start), end: Math.min(max, selection.end) }
      : null;

  const msAtClientX = (clientX) => {
    const rect = trackRef.current.getBoundingClientRect();
    const frac = rect.width ? (clientX - rect.left) / rect.width : 0;
    return fracToMs(frac, min, max);
  };

  const onPointerDown = (e) => {
    if (e.button !== 0) return;
    e.currentTarget.setPointerCapture?.(e.pointerId);
    const t = msAtClientX(e.clientX);
    setDrag({ a: t, b: t });
  };

  const onPointerMove = (e) => {
    if (!drag) return;
    setDrag((d) => (d ? { ...d, b: msAtClientX(e.clientX) } : d));
  };

  const onPointerUp = () => {
    if (!drag) return;
    const a = Math.min(drag.a, drag.b);
    const b = Math.max(drag.a, drag.b);
    setDrag(null);
    // A negligible drag = a click → clear the selection.
    if (b - a < span * 0.01) onClear();
    else onSelect(a, b);
  };

  return (
    <div className="timeline" aria-label="Data timeline">
      <div
        ref={trackRef}
        className="timeline-track"
        role="slider"
        aria-label="Drag to filter by time"
        aria-valuemin={min}
        aria-valuemax={max}
        aria-valuenow={band ? band.start : min}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
      >
        <div className="timeline-bars">
          {bins.map((c, i) => (
            <div
              key={i}
              className="timeline-bar"
              style={{ height: `${(c / peak) * 100}%` }}
            />
          ))}
        </div>
        {band && band.end > band.start && (
          <div
            className="timeline-band"
            style={{ left: pct(band.start), width: `${((band.end - band.start) / span) * 100}%` }}
          />
        )}
      </div>
      <div className="timeline-labels">
        <span>{fmtLabel(min)}</span>
        {band ? (
          <button type="button" className="timeline-clear" onClick={onClear}>
            {fmtLabel(band.start)} – {fmtLabel(band.end)} · clear
          </button>
        ) : (
          <span className="timeline-hint">drag to filter by time</span>
        )}
        <span>{fmtLabel(max)}</span>
      </div>
    </div>
  );
};

export default Timeline;
