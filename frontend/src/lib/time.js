// Time helpers shared by the Timeline scrubber and App's timespan wiring.

// Format an epoch-ms into the local 'YYYY-MM-DDTHH:mm' string that the
// datetime-local inputs and the subscribe/URL code (App.jsx) already use.
export const msToLocalInput = (ms) => {
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, '0');
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
    `T${pad(d.getHours())}:${pad(d.getMinutes())}`
  );
};

// Map a fractional x position (0..1) across a track to an epoch-ms in [min, max].
export const fracToMs = (frac, min, max) => min + Math.min(1, Math.max(0, frac)) * (max - min);
