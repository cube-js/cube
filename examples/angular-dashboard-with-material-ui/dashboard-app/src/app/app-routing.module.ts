import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { DashboardPageComponent } from './dashboard-page/dashboard-page.component';
import { TablePageComponent } from './table-page/table-page.component';

const routes: Routes = [
  { path: '', component: DashboardPageComponent },
  { path: 'table', component: TablePageComponent },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
