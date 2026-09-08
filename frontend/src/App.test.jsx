import { render, act, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import App from './App';

// Mock the RadiationMap component to prevent leaflet issues in jsdom. The basemap
// catalog now lives in ./components/basemaps, so this mock only needs the default.
vi.mock('./components/RadiationMap', () => ({
  default: ({ markers, blobs, alerts }) => <div data-testid="map-mock">Markers: {markers.length}, Blobs: {blobs?.length || 0}, Alerts: {alerts?.length || 0}</div>,
}));

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Build a minimal fetch mock that returns the given JSON body and status.
 */
const mockFetch = (body, status = 200) =>
  vi.fn().mockResolvedValue({
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  });

describe('App WebSocket Reconnection & Envelope Processing', () => {
  let wsInstances = [];

  beforeEach(() => {
    wsInstances = [];
    globalThis.WebSocket = class {
      constructor(url) {
        this.url = url;
        this.close = vi.fn();
        wsInstances.push(this);
      }
    };
    // Suppress GET /config fetch noise in tests that don't need it
    globalThis.fetch = mockFetch({ cpm_warn_threshold: 100, cpm_danger_threshold: 1000 });
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('unwraps the clean event envelope and stores data', () => {
    const { getByTestId } = render(<App />);
    expect(wsInstances.length).toBe(1);

    act(() => {
      wsInstances[0].onmessage({
        data: JSON.stringify({
          type: 'clean',
          data: { sensor_id: 'sensor-1', latitude: 10, longitude: 20 }
        })
      });
      vi.advanceTimersByTime(1000);
    });

    // The RadiationMap mock should now have 1 marker
    expect(getByTestId('map-mock').textContent).toBe('Markers: 1, Blobs: 0, Alerts: 0');
  });

  it('unwraps aggregated blobs and stores data', () => {
    const { getByTestId } = render(<App />);
    act(() => {
      wsInstances[0].onmessage({
        data: JSON.stringify({
          type: 'aggregated',
          data: { geohash: 'g1', window_start: '2026', count: 5 }
        })
      });
      vi.advanceTimersByTime(1000);
    });
    expect(getByTestId('map-mock').textContent).toBe('Markers: 0, Blobs: 1, Alerts: 0');
  });

  it('unwraps alert events and stores data', () => {
    const { getByTestId } = render(<App />);
    act(() => {
      wsInstances[0].onmessage({
        data: JSON.stringify({
          type: 'alert',
          data: { sensor_id: 'sensor-1', window_start: '2026', cpm: 500 }
        })
      });
      vi.advanceTimersByTime(1000);
    });
    expect(getByTestId('map-mock').textContent).toBe('Markers: 0, Blobs: 0, Alerts: 1');
  });

  it('renders an alert in the sidebar Alert Logs (no toast pop-up)', () => {
    const { container } = render(<App />);
    act(() => {
      wsInstances[0].onmessage({
        data: JSON.stringify({
          type: 'alert',
          data: {
            sensor_id: 'sensor-42',
            window_start: '2026-06-11T10:00:00',
            triggered_at: '2026-06-11T10:00:00',
            latitude: 35,
            longitude: 139,
            cpm: 1200,
          }
        })
      });
      vi.advanceTimersByTime(1000);
    });

    const rows = container.querySelectorAll('.alert-log-row');
    expect(rows.length).toBe(1);
    expect(rows[0].textContent).toContain('sensor-42');
    // The old toast container must be gone.
    expect(container.querySelector('.alert-toast-container')).toBeNull();
  });

  it('reconnects after 5 seconds on close, but not if unmounted', () => {
    const { unmount } = render(<App />);

    // Simulate close while mounted
    act(() => {
      wsInstances[0].onclose();
    });

    // Fast forward 5 seconds
    act(() => {
      vi.advanceTimersByTime(5000);
    });

    // A new instance should be created
    expect(wsInstances.length).toBe(2);

    // Unmount the component
    unmount();

    // Simulate close on the second instance
    act(() => {
      wsInstances[1].onclose();
    });

    // Fast forward 5 seconds
    act(() => {
      vi.advanceTimersByTime(5000);
    });

    // No new instance should be created because it's unmounted (isClosed = true)
    expect(wsInstances.length).toBe(2);
  });
});

describe('App — finding (a): GET /config on load seeds thresholds', () => {
  let wsInstances = [];

  beforeEach(() => {
    wsInstances = [];
    globalThis.WebSocket = class {
      constructor(url) { this.url = url; this.close = vi.fn(); wsInstances.push(this); }
    };
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('seeds warn and danger inputs from GET /config response', async () => {
    globalThis.fetch = mockFetch({
      cpm_warn_threshold: 250,
      cpm_danger_threshold: 800,
    });

    render(<App />);

    // Wait for the fetch to resolve and state to settle
    await act(async () => {
      await Promise.resolve();
    });

    const warnInput = document.getElementById('warn-threshold-input');
    const dangerInput = document.getElementById('danger-threshold-input');
    expect(Number(warnInput.value)).toBe(250);
    expect(Number(dangerInput.value)).toBe(800);
  });

  it('falls back gracefully when GET /config is unreachable', async () => {
    globalThis.fetch = vi.fn().mockRejectedValue(new Error('network error'));

    // Should not throw
    expect(() => render(<App />)).not.toThrow();
    await act(async () => { await Promise.resolve(); });
  });
});

describe('App — finding (b): markers replaced in place via Map (O(1))', () => {
  let wsInstances = [];

  beforeEach(() => {
    wsInstances = [];
    globalThis.WebSocket = class {
      constructor(url) { this.url = url; this.close = vi.fn(); wsInstances.push(this); }
    };
    globalThis.fetch = mockFetch({ cpm_warn_threshold: 100, cpm_danger_threshold: 1000 });
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('does not accumulate duplicate sensor_id entries', () => {
    const { getByTestId } = render(<App />);

    act(() => {
      // Send the same sensor_id twice — should only show 1 marker
      wsInstances[0].onmessage({
        data: JSON.stringify({ type: 'clean', data: { sensor_id: 'sensor-A', latitude: 1, longitude: 1, cpm: 10 } })
      });
      wsInstances[0].onmessage({
        data: JSON.stringify({ type: 'clean', data: { sensor_id: 'sensor-A', latitude: 1, longitude: 1, cpm: 20 } })
      });
      vi.advanceTimersByTime(1000);
    });

    expect(getByTestId('map-mock').textContent).toBe('Markers: 1, Blobs: 0, Alerts: 0');
  });

  it('shows distinct entries for different sensor_ids', () => {
    const { getByTestId } = render(<App />);

    act(() => {
      wsInstances[0].onmessage({
        data: JSON.stringify({ type: 'clean', data: { sensor_id: 'sensor-X', latitude: 1, longitude: 1 } })
      });
      wsInstances[0].onmessage({
        data: JSON.stringify({ type: 'clean', data: { sensor_id: 'sensor-Y', latitude: 2, longitude: 2 } })
      });
      vi.advanceTimersByTime(1000);
    });

    expect(getByTestId('map-mock').textContent).toBe('Markers: 2, Blobs: 0, Alerts: 0');
  });
});

describe('App — per-client view filters (REMAP model)', () => {
  let wsInstances = [];
  let originalLocalStorage;

  beforeEach(() => {
    wsInstances = [];
    globalThis.WebSocket = class {
      constructor(url) { this.url = url; this.close = vi.fn(); wsInstances.push(this); }
    };
    globalThis.fetch = mockFetch({ cpm_warn_threshold: 100, cpm_danger_threshold: 1000 });
    // vitest's jsdom exposes a non-functional localStorage stub — replace it
    // with a working in-memory Storage for the persistence assertions.
    originalLocalStorage = Object.getOwnPropertyDescriptor(window, 'localStorage');
    const store = new Map();
    Object.defineProperty(window, 'localStorage', {
      configurable: true,
      value: {
        getItem: (k) => (store.has(k) ? store.get(k) : null),
        setItem: (k, v) => store.set(k, String(v)),
        removeItem: (k) => store.delete(k),
        clear: () => store.clear(),
      },
    });
    window.history.replaceState(null, '', '/');
    vi.useFakeTimers();
  });

  afterEach(() => {
    if (originalLocalStorage) {
      Object.defineProperty(window, 'localStorage', originalLocalStorage);
    } else {
      delete window.localStorage;
    }
    window.history.replaceState(null, '', '/');
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('changing the view time range never writes to /config', async () => {
    const postSpy = vi.fn();
    globalThis.fetch = vi.fn((url, opts) => {
      if (opts && opts.method === 'POST') postSpy();
      return Promise.resolve({
        ok: true,
        status: 200,
        json: async () => ({ cpm_warn_threshold: 100, cpm_danger_threshold: 1000 }),
      });
    });

    render(<App />);
    await act(async () => { await Promise.resolve(); });

    act(() => {
      fireEvent.change(document.getElementById('view-start-input'), { target: { value: '2011-03-11T00:00' } });
      fireEvent.change(document.getElementById('view-end-input'), { target: { value: '2011-04-30T23:59' } });
    });

    // View filters are client-local: no POST may leave the browser.
    expect(postSpy).not.toHaveBeenCalled();
  });

  it('filters incoming markers by the local view timespan', () => {
    const { getByTestId } = render(<App />);

    act(() => {
      fireEvent.change(document.getElementById('view-start-input'), { target: { value: '2011-03-11T00:00' } });
      fireEvent.change(document.getElementById('view-end-input'), { target: { value: '2011-04-30T23:59' } });
    });

    act(() => {
      // One event inside the window, one outside — only the first may render.
      wsInstances[0].onmessage({
        data: JSON.stringify({
          type: 'clean',
          data: { sensor_id: 'in-window', latitude: 35, longitude: 139, captured_at: '2011-03-15T12:00:00' }
        })
      });
      wsInstances[0].onmessage({
        data: JSON.stringify({
          type: 'clean',
          data: { sensor_id: 'out-of-window', latitude: 35, longitude: 139, captured_at: '2020-01-01T00:00:00' }
        })
      });
      vi.advanceTimersByTime(1000);
    });

    expect(getByTestId('map-mock').textContent).toBe('Markers: 1, Blobs: 0, Alerts: 0');
  });

  it('sends a subscribe frame with the view filters on open and on change', async () => {
    // Give the mock socket a live readyState + send spy for this test.
    const sends = [];
    globalThis.WebSocket = class {
      constructor(url) {
        this.url = url;
        this.readyState = 1;
        this.close = vi.fn();
        this.send = (msg) => sends.push(JSON.parse(msg));
        wsInstances.push(this);
      }
    };

    render(<App />);
    await act(async () => { await Promise.resolve(); });

    act(() => {
      wsInstances[0].onopen();
    });
    // First subscribe on open: no filters yet.
    expect(sends.at(-1)).toEqual({ type: 'subscribe', area: null, timespan: null });

    act(() => {
      fireEvent.change(document.getElementById('view-start-input'), { target: { value: '2011-03-11T00:00' } });
      fireEvent.change(document.getElementById('view-end-input'), { target: { value: '2011-04-30T23:59' } });
    });

    const last = sends.at(-1);
    expect(last.type).toBe('subscribe');
    expect(last.timespan.start).toBe(new Date('2011-03-11T00:00').toISOString());
    expect(last.timespan.end).toBe(new Date('2011-04-30T23:59').toISOString());
  });

  it('persists the view to localStorage and mirrors it into the URL', () => {
    render(<App />);

    act(() => {
      fireEvent.change(document.getElementById('view-start-input'), { target: { value: '2011-03-11T00:00' } });
      fireEvent.change(document.getElementById('view-end-input'), { target: { value: '2011-04-30T23:59' } });
    });

    const stored = JSON.parse(window.localStorage.getItem('radiation-tracker.view.v1'));
    expect(stored.view.timespan.start).toBe('2011-03-11T00:00');
    expect(window.location.search).toContain('start=2011-03-11T00%3A00');
  });
});

describe('App — finding (c): threshold validation before POST', () => {
  let wsInstances = [];

  beforeEach(() => {
    wsInstances = [];
    globalThis.WebSocket = class {
      constructor(url) { this.url = url; this.close = vi.fn(); wsInstances.push(this); }
    };
    globalThis.fetch = mockFetch({ cpm_warn_threshold: 100, cpm_danger_threshold: 1000 });
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('shows error when warn >= danger and does not POST', async () => {
    // Track POST attempts while still answering the mount-time GET /config.
    const postSpy = vi.fn();
    globalThis.fetch = vi.fn((url, opts) => {
      if (opts && opts.method === 'POST') postSpy();
      return Promise.resolve({
        ok: true,
        status: 200,
        json: async () => ({ cpm_warn_threshold: 100, cpm_danger_threshold: 1000 }),
      });
    });

    render(<App />);
    // Flush the mount-time GET /config microtask.
    await act(async () => { await Promise.resolve(); });

    // Set warn > danger via the controlled inputs (fireEvent works under fake timers).
    const warnInput = document.getElementById('warn-threshold-input');
    const dangerInput = document.getElementById('danger-threshold-input');
    fireEvent.change(warnInput, { target: { value: '900' } });
    fireEvent.change(dangerInput, { target: { value: '100' } });

    await act(async () => {
      fireEvent.submit(document.getElementById('threshold-config-form'));
    });

    const errMsg = document.getElementById('config-error-msg');
    expect(errMsg).not.toBeNull();
    expect(errMsg.textContent).toMatch(/warn.*danger|threshold/i);
    // Client-side guard must short-circuit before any network write.
    expect(postSpy).not.toHaveBeenCalled();
  });

  it('does not show error when warn < danger', () => {
    render(<App />);

    const warnInput = document.getElementById('warn-threshold-input');
    const dangerInput = document.getElementById('danger-threshold-input');

    act(() => {
      fireEvent.change(warnInput, { target: { value: '100' } });
      fireEvent.change(dangerInput, { target: { value: '500' } });
      fireEvent.submit(document.getElementById('threshold-config-form'));
    });

    // No client-side error element should appear when inputs are valid
    const errMsg = document.getElementById('config-error-msg');
    expect(errMsg).toBeNull();
  });
});
