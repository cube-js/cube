import { HttpClientModule } from '@angular/common/http';
import { NgModule } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatGridListModule } from '@angular/material/grid-list';
import { MatIconModule } from '@angular/material/icon';
import { MatDividerModule } from '@angular/material/divider';
import { MatSelectModule } from '@angular/material/select';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { CubejsClientModule, QueryBuilderService } from '@cubejs-client/ngx';
import { ChartsModule } from 'ng2-charts';

import { AppComponent } from './app.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { QueryRendererComponent } from './dashboard/query-renderer/query-renderer.component';
import { MembersGroupComponent } from './dashboard/members-group/members-group.component';
import { TimeGroupComponent } from './dashboard/time-group/time-group.component';

const cubejsOptions = {
  token: 'environment.CUBEJS_API_TOKEN',
  options: {
    apiUrl: 'http://localhost:4000/cubejs-api/v1',
  },
};

@NgModule({
  declarations: [AppComponent, DashboardComponent, QueryRendererComponent, MembersGroupComponent, TimeGroupComponent],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    CubejsClientModule.forRoot(cubejsOptions),
    MatButtonModule,
    MatSelectModule,
    MatGridListModule,
    MatIconModule,
    MatDividerModule,
    HttpClientModule,
    ChartsModule,
  ],
  providers: [QueryBuilderService],
  bootstrap: [AppComponent],
})
export class AppModule {
  constructor() {}
}
