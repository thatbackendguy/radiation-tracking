import { render } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import RadiationMap from './components/RadiationMap';

// Mock react-leaflet components since they require a real DOM with actual sizing.
// useMap() must expose the methods ResizeHandler/FlyToController call.
vi.mock('react-leaflet', () => ({
  MapContainer: ({ children }) => <div data-testid="map-container">{children}</div>,
  TileLayer: () => <div data-testid="tile-layer" />,
  CircleMarker: ({ center, pathOptions }) => (
    <div data-testid="circle-marker" data-center={center.join(',')} data-color={pathOptions.color} />
  ),
  Popup: ({ children }) => <div data-testid="popup">{children}</div>,
  useMap: () => ({
    invalidateSize: vi.fn(),
    getZoom: vi.fn(() => 2),
    flyTo: vi.fn(),
  })
}));

describe('RadiationMap Component', () => {
  it('renders markers with correct coordinates and colors based on classification', () => {
    const mockMarkers = [
      { sensor_id: '1', latitude: 35.6895, longitude: 139.6917, classification: 'SAFE', cpm: 45 },
      { sensor_id: '2', latitude: 35.7, longitude: 139.7, classification: 'DANGER', cpm: 1000 }
    ];
    const defaultLayers = { markers: true, blobs: true, alerts: true };

    const { getAllByTestId } = render(
      <RadiationMap markers={mockMarkers} blobs={[]} alerts={[]} layers={defaultLayers} />
    );
    
    const renderedMarkers = getAllByTestId('circle-marker');
    expect(renderedMarkers.length).toBe(2);
    
    expect(renderedMarkers[0].getAttribute('data-center')).toBe('35.6895,139.6917');
    expect(renderedMarkers[0].getAttribute('data-color')).toBe('green'); // SAFE
    
    expect(renderedMarkers[1].getAttribute('data-center')).toBe('35.7,139.7');
    expect(renderedMarkers[1].getAttribute('data-color')).toBe('red'); // DANGER
  });
});
