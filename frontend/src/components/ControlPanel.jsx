import { useState } from 'react';
import Legend from './Legend';
import HistoryInsights from './insights/HistoryInsights';
import './ControlPanel.css';

// Collapsible section. The body stays mounted (CSS collapse) so form state and
// element ids survive open/close.
const Section = ({ title, badge, defaultOpen = true, children }) => {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <section className="panel-section">
      <button
        type="button"
        className="section-header"
        aria-expanded={open}
        onClick={() => setOpen(o => !o)}
      >
        <span className="section-title">{title}</span>
        {badge && <span className="section-badge">{badge}</span>}
        <span className={`chevron ${open ? 'open' : ''}`} aria-hidden="true">▾</span>
      </button>
      <div className={`section-body ${open ? '' : 'collapsed'}`}>{children}</div>
    </section>
  );
};

const alertReason = (alert) =>
  alert.reason && alert.reason.toLowerCase().includes('trend') ? 'Rising trend' : 'Sustained high';

const fmtAlertTime = (alert) => {
  const raw = alert.triggered_at || alert.window_start;
  const t = raw ? new Date(raw).getTime() : NaN;
  return Number.isFinite(t) ? new Date(t).toLocaleString() : '—';
};

/**
 * REMAP-style control panel. Everything under "My view" is per-client state
 * applied locally in the browser; only the "Pipeline config" section writes to
 * the backend (global, token-gated).
 */
const ControlPanel = ({
  open,
  view,
  setView,
  layers,
  setLayers,
  refreshCadence,
  setRefreshCadence,
  displayThresholds,
  pipelineThresholds,
  setPipelineThresholds,
  onPipelineSubmit,
  configError,
  configSaved,
  alerts = [],
  onFocusAlert,
  apiBase,
}) => {
  const timespanSet = Boolean(view.timespan.start || view.timespan.end);
  // Newest first, capped so the list stays light on a long soak.
  const alertLog = [...alerts].reverse().slice(0, 50);

  return (
    <aside className={`control-panel ${open ? '' : 'closed'}`} aria-label="Control panel" aria-hidden={!open}>
      <div className="panel-scroll">
        <Section title="My view" badge="only you">
          <p className="section-hint">
            Filters apply to your session only — other clients keep their own view.
            Share your current view by copying the page URL.
          </p>

          <fieldset className="field-block">
            <legend>Time range</legend>
            <label htmlFor="view-start-input">From
              <input
                id="view-start-input"
                type="datetime-local"
                aria-label="View timespan start"
                value={view.timespan.start}
                onChange={e => setView(v => ({ ...v, timespan: { ...v.timespan, start: e.target.value } }))}
              />
            </label>
            <label htmlFor="view-end-input">To
              <input
                id="view-end-input"
                type="datetime-local"
                aria-label="View timespan end"
                value={view.timespan.end}
                onChange={e => setView(v => ({ ...v, timespan: { ...v.timespan, end: e.target.value } }))}
              />
            </label>
            {timespanSet && (
              <button
                type="button"
                id="clear-timespan-btn"
                className="ghost-btn"
                onClick={() => setView(v => ({ ...v, timespan: { start: '', end: '' } }))}
              >
                Clear time range
              </button>
            )}
          </fieldset>

          <fieldset className="field-block">
            <legend>Colour scale (local)</legend>
            <div className="inline-fields">
              <label htmlFor="display-warn-input">Warn ≥
                <input
                  id="display-warn-input"
                  type="number"
                  min="0"
                  placeholder="pipeline"
                  aria-label="Local warn colour threshold"
                  value={view.display.warn}
                  onChange={e => setView(v => ({ ...v, display: { ...v.display, warn: e.target.value } }))}
                />
              </label>
              <label htmlFor="display-danger-input">Danger ≥
                <input
                  id="display-danger-input"
                  type="number"
                  min="0"
                  placeholder="pipeline"
                  aria-label="Local danger colour threshold"
                  value={view.display.danger}
                  onChange={e => setView(v => ({ ...v, display: { ...v.display, danger: e.target.value } }))}
                />
              </label>
            </div>
            <p className="section-hint">
              Recolours markers on this map by CPM. Leave blank to use the pipeline&apos;s
              classification. Alerts are unaffected.
            </p>
          </fieldset>
        </Section>

        <Section title="Alert Logs" badge={alertLog.length ? String(alertLog.length) : undefined}>
          {alertLog.length === 0 ? (
            <p className="section-hint">No alerts yet.</p>
          ) : (
            <ul className="alert-logs" aria-label="Alert logs">
              {alertLog.map((alert) => (
                <li key={`${alert.sensor_id}_${alert.window_start}`}>
                  <button
                    type="button"
                    className="alert-log-row"
                    onClick={() => onFocusAlert?.(alert)}
                    title="Zoom the map to this alert"
                  >
                    <span className="alert-log-top">
                      <span className="alert-log-sensor">{alert.sensor_id}</span>
                      <span className="alert-log-cpm">
                        {alert.cpm != null ? `${Number(alert.cpm).toFixed(0)} CPM` : '—'}
                      </span>
                    </span>
                    <span className="alert-log-meta">
                      <span className="alert-log-reason">{alertReason(alert)}</span>
                      <span className="alert-log-time">{fmtAlertTime(alert)}</span>
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </Section>

        <Section title="Layers & refresh">
          <div className="layer-toggles" aria-label="Layer Toggles" role="group">
            <label>
              <input
                type="checkbox"
                aria-label="Toggle Raw Markers"
                checked={layers.markers}
                onChange={e => setLayers({ ...layers, markers: e.target.checked })}
              /> Raw sensor markers
            </label>
            <label>
              <input
                type="checkbox"
                aria-label="Toggle Density Blobs"
                checked={layers.blobs}
                onChange={e => setLayers({ ...layers, blobs: e.target.checked })}
              /> Density blobs
            </label>
            <label>
              <input
                type="checkbox"
                aria-label="Toggle Alerts"
                checked={layers.alerts}
                onChange={e => setLayers({ ...layers, alerts: e.target.checked })}
              /> Alerts
            </label>
          </div>
          <label className="cadence-row" htmlFor="refresh-cadence-select">Refresh
            <select
              id="refresh-cadence-select"
              aria-label="Refresh Cadence"
              value={refreshCadence}
              onChange={e => setRefreshCadence(Number(e.target.value))}
            >
              <option value={100}>Live (100ms)</option>
              <option value={1000}>1s</option>
              <option value={5000}>5s</option>
              <option value={10000}>10s</option>
            </select>
          </label>
        </Section>

        <Section title="Legend">
          <Legend
            thresholds={
              displayThresholds ?? {
                warn: Number(pipelineThresholds.warn) || null,
                danger: Number(pipelineThresholds.danger) || null,
              }
            }
            localScale={Boolean(displayThresholds)}
          />
        </Section>

        <Section title="Insights" badge="archive">
          <p className="section-hint">
            Historical trends from the Database archive, scoped to your
            current view. Click a hotspot to zoom the map to it.
          </p>
          <HistoryInsights
            apiBase={apiBase}
            view={view}
            thresholds={
              displayThresholds ?? {
                warn: Number(pipelineThresholds.warn) || 100,
                danger: Number(pipelineThresholds.danger) || 1000,
              }
            }
            onFocus={onFocusAlert}
          />
        </Section>

        <Section title="Pipeline config" badge="admin · global" defaultOpen={false}>
          <p className="section-hint section-warning">
            These thresholds drive alerting in the Flink pipeline for <strong>every client</strong>.
            Writes are token-gated on the public deployment.
          </p>
          <form id="threshold-config-form" className="threshold-form" onSubmit={onPipelineSubmit}>
            <div className="inline-fields" aria-label="Threshold Filters">
              <label htmlFor="warn-threshold-input">Warn
                <input
                  id="warn-threshold-input"
                  type="number"
                  aria-label="Warning Threshold"
                  value={pipelineThresholds.warn}
                  onChange={e => setPipelineThresholds({ ...pipelineThresholds, warn: e.target.value })}
                />
              </label>
              <label htmlFor="danger-threshold-input">Danger
                <input
                  id="danger-threshold-input"
                  type="number"
                  aria-label="Danger Threshold"
                  value={pipelineThresholds.danger}
                  onChange={e => setPipelineThresholds({ ...pipelineThresholds, danger: e.target.value })}
                />
              </label>
            </div>
            <button id="threshold-submit-btn" type="submit" aria-label="Update Pipeline Thresholds">
              Apply globally
            </button>
            {configError && (
              <p id="config-error-msg" role="alert" className="config-error" aria-live="assertive">
                {configError}
              </p>
            )}
            {configSaved && !configError && (
              <p id="config-saved-msg" role="status" className="config-saved" aria-live="polite">
                Published to the pipeline.
              </p>
            )}
          </form>
        </Section>
      </div>

      <footer className="panel-footer">
        Streaming measurements are unvalidated raw data — elevated readings alone are not
        evidence of increased radioactivity.
      </footer>
    </aside>
  );
};

export default ControlPanel;
