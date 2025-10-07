import { Component, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { GoogleMap, GoogleMapsModule } from '@angular/google-maps';

type Waypoint = { lat: number; lng: number; label?: string };
type RouteData = { waypoints: Waypoint[] };

@Component({
  selector: 'app-map',
  standalone: true,
  imports: [CommonModule, GoogleMapsModule],
  templateUrl: './map.component.html',
  styleUrl: './map.component.css',
})
export class MapComponent {
  @ViewChild(GoogleMap, { static: true }) map?: GoogleMap;

  loading = true;
  error?: string;

  markers: google.maps.LatLngLiteral[] = [];
  path: google.maps.LatLngLiteral[] = [];

  options: google.maps.MapOptions = {
    mapTypeId: 'roadmap',
    disableDefaultUI: true,
    clickableIcons: false,
  };

  polylineOptions: google.maps.PolylineOptions = {
    strokeColor: '#1a73e8',
    strokeOpacity: 0.9,
    strokeWeight: 4,
  };

  markerOptions: google.maps.MarkerOptions = {
    icon: {
      path: google.maps.SymbolPath.CIRCLE,
      scale: 4, // small dot
      fillColor: '#1a73e8',
      fillOpacity: 1,
      strokeColor: '#1a73e8',
      strokeOpacity: 1,
      strokeWeight: 1,
    },
    clickable: false,
  };

  constructor(private http: HttpClient) {
    this.http.get<RouteData>('route.json').subscribe({
      next: (data) => {
        const points = (data?.waypoints || []).filter(
          (p) => typeof p.lat === 'number' && typeof p.lng === 'number'
        );
        this.markers = points.map((p) => ({ lat: p.lat, lng: p.lng }));
        this.path = [...this.markers];
        this.fitToBounds();
        this.loading = false;
      },
      error: (err) => {
        this.error = 'Failed to load route.json';
        this.loading = false;
        console.error(err);
      },
    });
  }

  private fitToBounds() {
    if (!this.map || this.markers.length === 0) return;
    const bounds = new google.maps.LatLngBounds();
    this.markers.forEach((m) => bounds.extend(m));
    if (this.markers.length === 1) {
      this.map.center = this.markers[0];
      this.map.zoom = 14;
    } else {
      this.map.fitBounds(bounds);
    }
  }
}
