import { Component, ViewChild, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { GoogleMap, GoogleMapsModule } from '@angular/google-maps';
import { Subscription, interval, of } from 'rxjs';
import { catchError, startWith, switchMap } from 'rxjs/operators';

type Waypoint = { lat: number; lng: number; label?: string };
type RouteData = { waypoints: Waypoint[] };

@Component({
  selector: 'app-map',
  standalone: true,
  imports: [CommonModule, GoogleMapsModule],
  templateUrl: './map.component.html',
  styleUrl: './map.component.css',
})
export class MapComponent implements OnDestroy {
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

  private pollSub?: Subscription;
  private lastKey = '';

  constructor(private http: HttpClient) {
    this.pollSub = interval(1000)
      .pipe(
        startWith(0),
        switchMap(() =>
          this.http
            .get<RouteData>(`route.json?t=${Date.now()}`)
            .pipe(catchError((err) => {
              this.error = 'Failed to load route.json';
              console.error(err);
              return of(undefined as unknown as RouteData);
            }))
        )
      )
      .subscribe((data) => {
        if (!data || !Array.isArray((data as any).waypoints)) {
          this.loading = false;
          return;
        }
        const points = (data.waypoints || []).filter(
          (p: any) => typeof p.lat === 'number' && typeof p.lng === 'number'
        );
        const key = JSON.stringify(points);
        if (key !== this.lastKey) {
          this.markers = points.map((p) => ({ lat: p.lat, lng: p.lng }));
          this.path = [...this.markers];
          this.fitToBounds();
          this.lastKey = key;
          this.error = undefined;
        }
        this.loading = false;
      });
  }

  ngOnDestroy(): void {
    this.pollSub?.unsubscribe();
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
