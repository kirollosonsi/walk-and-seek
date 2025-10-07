import { Routes } from '@angular/router';

export const routes: Routes = [
  {
    path: '',
    loadComponent: () => import('./map/map.component').then((m) => m.MapComponent),
  },
];
