import './Legend.css';

// Renders inline inside the control panel. `thresholds` labels the colour
// bands with concrete CPM bounds; `localScale` marks that the client's own
// colour scale (not the pipeline classification) is active.
const Legend = ({ thresholds = { warn: null, danger: null }, localScale = false }) => {
  const { warn, danger } = thresholds;
  return (
    <div className="legend-container" aria-label="Map Legend">
      <div className="legend-item">
        <span className="legend-color safe" aria-hidden="true"></span>
        <span className="legend-label">Safe{warn != null ? ` · < ${warn} CPM` : ''}</span>
      </div>
      <div className="legend-item">
        <span className="legend-color warn" aria-hidden="true"></span>
        <span className="legend-label">
          Warn{warn != null && danger != null ? ` · ${warn}–${danger} CPM` : ''}
        </span>
      </div>
      <div className="legend-item">
        <span className="legend-color danger" aria-hidden="true"></span>
        <span className="legend-label">Danger{danger != null ? ` · ≥ ${danger} CPM` : ''}</span>
      </div>
      <div className="legend-item">
        <span className="legend-blob" aria-hidden="true"></span>
        <span className="legend-label">Density blob (aggregated)</span>
      </div>
      <div className="legend-item">
        <span className="legend-alert" aria-hidden="true"></span>
        <span className="legend-label">Active alert</span>
      </div>
      {localScale && (
        <p className="legend-note">Using your local colour scale — other clients are unaffected.</p>
      )}
    </div>
  );
};

export default Legend;
