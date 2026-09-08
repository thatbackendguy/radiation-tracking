import { useEffect } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import { BASEMAP_BY_ID, DEFAULT_BASEMAP } from './basemaps';

// The map shares a flex row with the collapsible panel, so Leaflet must be told
// to re-measure its container after the panel's width transition finishes.
const ResizeHandler = ({ trigger }) => {
  const map = useMap();

  useEffect(() => {
    const id = setTimeout(() => map.invalidateSize?.(), 260);
    return () => clearTimeout(id);
  }, [map, trigger]);

  return null;
};

// Flies the map to a chosen alert marker. `target.key` is a nonce so re-clicking
// the same alert re-triggers the animation.
const FlyToController = ({ target }) => {
  const map = useMap();

  useEffect(() => {
    if (!target || target.lat == null || target.lon == null) return;
    const zoom = Math.max(map.getZoom?.() ?? 2, 8);
    map.flyTo?.([target.lat, target.lon], zoom, { duration: 0.8 });
  }, [map, target]);

  return null;
};

const RadiationMap = ({
  markers,
  blobs,
  alerts,
  layers,
  displayThresholds = null,
  basemap = DEFAULT_BASEMAP,
  panelOpen = true,
  focusTarget = null,
}) => {
  // Local display thresholds (per-client colour scale) win when set; otherwise
  // fall back to the classification computed by the pipeline.
  const getColor = (classification, cpm) => {
    if (displayThresholds && cpm != null) {
      if (cpm >= displayThresholds.danger) return 'red';
      if (cpm >= displayThresholds.warn) return 'orange';
      return 'green';
    }
    if (classification === 'DANGER') return 'red';
    if (classification === 'WARN') return 'orange';
    return 'green';
  };

  const tiles = BASEMAP_BY_ID[basemap] || BASEMAP_BY_ID[DEFAULT_BASEMAP];

  return (
    <MapContainer id="radiation-map-container" center={[20, 0]} zoom={2} style={{ height: '100%', width: '100%' }}>
      <TileLayer key={basemap} url={tiles.url} attribution={tiles.attribution} maxZoom={tiles.maxZoom} />
      <ResizeHandler trigger={panelOpen} />
      <FlyToController target={focusTarget} />

      {layers.markers && markers.map((marker) => (
        <CircleMarker
          key={`m_${marker.sensor_id}`}
          center={[marker.latitude, marker.longitude]}
          pathOptions={{
            color: getColor(marker.classification, marker.cpm),
            fillColor: getColor(marker.classification, marker.cpm),
            fillOpacity: 0.7,
            weight: 1.5
          }}
          radius={5}
        >
          <Popup>
            Sensor: {marker.sensor_id} <br />
            CPM: {marker.cpm} <br />
            Time: {marker.captured_at} <br />
            Status: {marker.classification}
          </Popup>
        </CircleMarker>
      ))}

      {layers.blobs && blobs.filter(b => b.centroid_latitude != null && b.centroid_longitude != null).map((blob) => (
        <CircleMarker
          key={`b_${blob.geohash}_${blob.window_start}`}
          center={[blob.centroid_latitude, blob.centroid_longitude]}
          pathOptions={{
            color: getColor(blob.worst_classification, blob.cpm_avg),
            fillColor: getColor(blob.worst_classification, blob.cpm_avg),
            fillOpacity: 0.3
          }}
          radius={Math.max(10, Math.min(50, blob.count * 2))}
        >
          <Popup>
            Geohash: {blob.geohash} <br />
            Count: {blob.count} <br />
            Avg CPM: {blob.cpm_avg?.toFixed(2)} <br />
            Time: {blob.window_start}
          </Popup>
        </CircleMarker>
      ))}

      {layers.alerts && alerts.filter(a => a.latitude != null && a.longitude != null).map((alert) => (
        <CircleMarker
          key={`a_${alert.sensor_id}_${alert.window_start}`}
          center={[alert.latitude, alert.longitude]}
          pathOptions={{ color: 'red', fillColor: 'red', fillOpacity: 0.8, weight: 3 }}
          radius={8}
        >
          <Popup>
            ALERT: {alert.sensor_id} <br />
            Peak CPM: {alert.cpm} <br />
            Breaches: {alert.breach_count} <br />
            Time: {alert.triggered_at}
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  );
};

export default RadiationMap;
