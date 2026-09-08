import { describe, it, expect } from 'vitest';
import { msToLocalInput, fracToMs } from '../lib/time';

describe('Timeline helpers', () => {
  it('msToLocalInput formats epoch-ms to a local YYYY-MM-DDTHH:mm string', () => {
    // Build a local time explicitly so the assertion is timezone-independent.
    const d = new Date(2011, 2, 11, 14, 46); // 2011-03-11 14:46 local
    expect(msToLocalInput(d.getTime())).toBe('2011-03-11T14:46');
  });

  it('msToLocalInput round-trips through Date parsing to the same minute', () => {
    const ms = new Date(2026, 5, 1, 9, 5).getTime();
    const str = msToLocalInput(ms);
    expect(new Date(str).getTime()).toBe(ms);
  });

  it('fracToMs maps 0..1 across the domain and clamps out-of-range fractions', () => {
    const min = 1000;
    const max = 5000;
    expect(fracToMs(0, min, max)).toBe(1000);
    expect(fracToMs(1, min, max)).toBe(5000);
    expect(fracToMs(0.5, min, max)).toBe(3000);
    expect(fracToMs(-1, min, max)).toBe(1000); // clamped low
    expect(fracToMs(2, min, max)).toBe(5000); // clamped high
  });
});
