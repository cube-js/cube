import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgModule } from '@angular/core';
import { CubejsClientModule } from '@cubejs-client/ngx';
import { MatCardModule } from '@angular/material/Card';

import { AppComponent } from './app.component';
import { ChartsModule } from 'ng2-charts';
import { BarChartComponent } from './bar-chart/bar-chart.component';
import { MatGridListModule } from '@angular/material/grid-list';
import { MatMenuModule } from '@angular/material/menu';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { LayoutModule } from '@angular/cdk/layout';
import { DashboardPageComponent } from './dashboard-page/dashboard-page.component';

const cubejsOptions = {
  token: 'YOUR-CUBEJS-API-TOKEN',
  options: { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
};

@NgModule({
  declarations: [
    AppComponent,
    BarChartComponent,
    DashboardPageComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    ChartsModule,
    CubejsClientModule.forRoot(cubejsOptions),
    MatCardModule,
    MatGridListModule,
    MatMenuModule,
    MatIconModule,
    MatButtonModule,
    LayoutModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
