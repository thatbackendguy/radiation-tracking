// Basemap catalog — one source of truth shared by RadiationMap, BasemapPicker
// and App. Order defines the picker order. `{r}` = retina suffix (@2x),
// `{s}` = subdomain. OSM/OpenTopo label in the local script; CARTO + Esri are
// English-friendly.
export const BASEMAPS = [
  {
    id: 'osm',
    label: 'OpenStreetMap',
    url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution: '&copy; OpenStreetMap contributors',
    maxZoom: 19,
  },
  {
    id: 'positron',
    label: 'Carto Positron',
    url: 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
    attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
    maxZoom: 20,
  },
  {
    id: 'darkmatter',
    label: 'Carto Dark Matter',
    url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
    maxZoom: 20,
  },
  {
    id: 'esri',
    label: 'Esri World Imagery',
    // Esri uses {z}/{y}/{x} ordering and has no {s} subdomain.
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attribution: 'Tiles &copy; Esri — Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community',
    maxZoom: 19,
  },
  {
    id: 'opentopo',
    label: 'OpenTopoMap',
    url: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
    attribution: '&copy; OpenStreetMap contributors, SRTM — &copy; OpenTopoMap (CC-BY-SA)',
    maxZoom: 17,
  },
];

export const DEFAULT_BASEMAP = 'darkmatter';

export const BASEMAP_BY_ID = Object.fromEntries(BASEMAPS.map((b) => [b.id, b]));
