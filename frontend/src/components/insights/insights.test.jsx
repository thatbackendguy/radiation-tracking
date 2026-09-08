import { render, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import TrendChart from './TrendChart';
import HotspotBars from './HotspotBars';

describe('TrendChart', () => {
  it('shows a fallback when there are fewer than two windows', () => {
    const { container } = render(<TrendChart points={[{ window_start: '2026-06-01T00:00:00Z', avg_cpm: 10, max_cpm: 20 }]} />);
    expect(container.querySelector('.trend-line')).toBeNull();
    expect(container.textContent).toMatch(/Not enough/i);
  });

  it('draws the avg line and marks anomaly windows', () => {
    const points = [
      { window_start: '2026-06-01T00:00:00Z', avg_cpm: 10, max_cpm: 20, anomaly_cells: 0 },
      { window_start: '2026-06-01T00:01:00Z', avg_cpm: 40, max_cpm: 90, anomaly_cells: 2 },
      { window_start: '2026-06-01T00:02:00Z', avg_cpm: 25, max_cpm: 50, anomaly_cells: 0 },
    ];
    const { container } = render(<TrendChart points={points} />);
    expect(container.querySelector('.trend-line')).not.toBeNull();
    expect(container.querySelector('.trend-band')).not.toBeNull();
    // Exactly one anomaly window → one anomaly marker.
    expect(container.querySelectorAll('.trend-anomaly').length).toBe(1);
  });
});

describe('HotspotBars', () => {
  const thresholds = { warn: 100, danger: 1000 };

  it('shows an empty state with no hotspots', () => {
    const { container } = render(<HotspotBars hotspots={[]} thresholds={thresholds} onFocus={vi.fn()} />);
    expect(container.querySelector('.hotspot-row')).toBeNull();
    expect(container.textContent).toMatch(/No hotspots/i);
  });

  it('colours bars by CPM band and flies to the cell on click', () => {
    const onFocus = vi.fn();
    const hotspots = [
      { geohash: 'xn774c', peak_cpm: 1500, latitude: 37.4, longitude: 140.1, anomaly_windows: 3 },
      { geohash: 'xn0u4', peak_cpm: 250, latitude: 36.0, longitude: 139.0, anomaly_windows: 0 },
      { geohash: 'svbfs', peak_cpm: 40, latitude: 35.0, longitude: 138.0, anomaly_windows: 0 },
    ];
    const { container } = render(<HotspotBars hotspots={hotspots} thresholds={thresholds} onFocus={onFocus} />);

    const rows = container.querySelectorAll('.hotspot-row');
    expect(rows.length).toBe(3);
    expect(container.querySelector('.hotspot-fill.band-danger')).not.toBeNull(); // 1500 ≥ danger
    expect(container.querySelector('.hotspot-fill.band-warn')).not.toBeNull(); // 250 ≥ warn
    expect(container.querySelector('.hotspot-fill.band-safe')).not.toBeNull(); // 40 < warn

    fireEvent.click(rows[0]);
    expect(onFocus).toHaveBeenCalledWith({ latitude: 37.4, longitude: 140.1 });
  });

  it('disables focus for a cell without a centroid', () => {
    const onFocus = vi.fn();
    const { container } = render(
      <HotspotBars hotspots={[{ geohash: 'abc', peak_cpm: 500 }]} thresholds={thresholds} onFocus={onFocus} />
    );
    const row = container.querySelector('.hotspot-row');
    expect(row.disabled).toBe(true);
    fireEvent.click(row);
    expect(onFocus).not.toHaveBeenCalled();
  });
});
