import { BASEMAPS } from './basemaps';
import './BasemapPicker.css';

// Floating radio group over the map's top-right corner (just below the topbar's
// "Live" pill) that switches the active basemap. State lives in App.
const BasemapPicker = ({ basemap, setBasemap }) => (
  <fieldset className="basemap-picker" role="radiogroup" aria-label="Basemap">
    <legend className="basemap-picker-title">Basemap</legend>
    {BASEMAPS.map((b) => (
      <label key={b.id} className="basemap-option">
        <input
          type="radio"
          name="basemap"
          value={b.id}
          checked={basemap === b.id}
          onChange={() => setBasemap(b.id)}
        />
        <span>{b.label}</span>
      </label>
    ))}
  </fieldset>
);

export default BasemapPicker;
