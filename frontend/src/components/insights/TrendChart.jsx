// CPM-over-time trend: a max-CPM envelope (area) behind the count-weighted
// avg-CPM line, with anomaly windows marked. One measure (CPM), one y-axis —
// avg and max share the same scale, so this is not a dual-axis chart.
const W = 300;
const H = 120;
const PAD = { top: 8, right: 8, bottom: 16, left: 30 };

const fmtTime = (ms) =>
  new Date(ms).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
const fmtHM = (ms) => new Date(ms).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });

const TrendChart = ({ points }) => {
  const data = (points || [])
    .map((p) => ({
      t: new Date(p.window_start).getTime(),
      avg: p.avg_cpm == null ? null : Number(p.avg_cpm),
      max: p.max_cpm == null ? null : Number(p.max_cpm),
      anomaly: (p.anomaly_cells || 0) > 0,
    }))
    .filter((d) => Number.isFinite(d.t))
    .sort((a, b) => a.t - b.t);

  if (data.length < 2) {
    return <p className="insights-empty">Not enough archived windows yet for a trend.</p>;
  }

  const t0 = data[0].t;
  const t1 = data[data.length - 1].t;
  const tSpan = t1 - t0 || 1;
  const yMax = Math.max(1, ...data.map((d) => d.max ?? d.avg ?? 0));

  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const x = (t) => PAD.left + ((t - t0) / tSpan) * plotW;
  const y = (v) => PAD.top + plotH - (v / yMax) * plotH;

  const avgPts = data.filter((d) => d.avg != null);
  const maxPts = data.filter((d) => d.max != null);
  const line = (pts, key) => pts.map((d, i) => `${i === 0 ? 'M' : 'L'}${x(d.t).toFixed(1)},${y(d[key]).toFixed(1)}`).join(' ');
  // Max envelope: max line out, back along the baseline.
  const area =
    maxPts.length > 1
      ? `${line(maxPts, 'max')} L${x(maxPts[maxPts.length - 1].t).toFixed(1)},${y(0).toFixed(1)} L${x(maxPts[0].t).toFixed(1)},${y(0).toFixed(1)} Z`
      : null;

  // ~3 y gridlines.
  const yTicks = [0, yMax / 2, yMax];

  return (
    <figure className="trend-figure">
      <svg viewBox={`0 0 ${W} ${H}`} className="trend-svg" role="img" aria-label="CPM over time">
        {yTicks.map((v, i) => (
          <g key={i}>
            <line className="trend-grid" x1={PAD.left} x2={W - PAD.right} y1={y(v)} y2={y(v)} />
            <text className="trend-axis" x={PAD.left - 4} y={y(v) + 3} textAnchor="end">
              {Math.round(v)}
            </text>
          </g>
        ))}
        {area && <path className="trend-band" d={area} />}
        <path className="trend-line" d={line(avgPts, 'avg')} />
        {data.filter((d) => d.anomaly && d.avg != null).map((d, i) => (
          <circle key={i} className="trend-anomaly" cx={x(d.t)} cy={y(d.avg)} r={2.6}>
            <title>{`Anomaly · ${fmtTime(d.t)} · avg ${Math.round(d.avg)} CPM`}</title>
          </circle>
        ))}
        <text className="trend-axis" x={PAD.left} y={H - 4} textAnchor="start">{fmtHM(t0)}</text>
        <text className="trend-axis" x={W - PAD.right} y={H - 4} textAnchor="end">{fmtHM(t1)}</text>
      </svg>
      <figcaption className="trend-legend">
        <span><span className="swatch swatch-line" /> avg CPM</span>
        <span><span className="swatch swatch-band" /> max</span>
        <span><span className="swatch swatch-anomaly" /> anomaly</span>
      </figcaption>
    </figure>
  );
};

export default TrendChart;
