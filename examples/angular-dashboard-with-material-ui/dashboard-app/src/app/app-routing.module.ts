import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { DashboardPageComponent } from './dashboard-page/dashboard-page.component';
import { KpiCardComponent } from './kpi-card/kpi-card.component';

const routes: Routes = [
  { path: '', component: DashboardPageComponent },
  { path: 'table', component: KpiCardComponent },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
