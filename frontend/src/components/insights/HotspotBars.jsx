// Top geohash cells by peak CPM — a magnitude ranking, so horizontal bars with
// direct value labels. Bars are coloured by the CPM band (safe/warn/danger, the
// same status semantics as the map legend), which is a genuine second signal, not
// decoration. Clicking a row flies the map to that cell's centroid.
const band = (cpm, thresholds) => {
  if (cpm == null) return 'safe';
  if (cpm >= thresholds.danger) return 'danger';
  if (cpm >= thresholds.warn) return 'warn';
  return 'safe';
};

const HotspotBars = ({ hotspots, thresholds, onFocus }) => {
  const rows = (hotspots || []).filter((h) => h.peak_cpm != null);
  if (rows.length === 0) {
    return <p className="insights-empty">No hotspots in the archived range yet.</p>;
  }
  const peak = Math.max(1, ...rows.map((h) => Number(h.peak_cpm)));

  return (
    <ul className="hotspot-bars" aria-label="Top cells by peak CPM">
      {rows.map((h) => {
        const cpm = Number(h.peak_cpm);
        const pct = Math.max(3, (cpm / peak) * 100);
        const cls = band(cpm, thresholds);
        const canFocus = h.latitude != null && h.longitude != null;
        return (
          <li key={h.geohash}>
            <button
              type="button"
              className="hotspot-row"
              disabled={!canFocus}
              onClick={() => canFocus && onFocus?.({ latitude: h.latitude, longitude: h.longitude })}
              title={canFocus ? 'Zoom the map to this cell' : 'No centroid for this cell'}
            >
              <span className="hotspot-label">
                <code>{h.geohash}</code>
                {h.anomaly_windows > 0 && <span className="hotspot-flag" title={`${h.anomaly_windows} anomaly windows`}>⚠</span>}
              </span>
              <span className="hotspot-track">
                <span className={`hotspot-fill band-${cls}`} style={{ width: `${pct}%` }} />
              </span>
              <span className="hotspot-value">{Math.round(cpm)}</span>
            </button>
          </li>
        );
      })}
    </ul>
  );
};

export default HotspotBars;
