import { HttpClient, HttpClientModule } from '@angular/common/http';
import { APP_INITIALIZER, NgModule } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatTabsModule } from '@angular/material/tabs';
import { MatGridListModule } from '@angular/material/grid-list';
import { MatIconModule } from '@angular/material/icon';
import { MatDialogModule } from '@angular/material/dialog';
import { MatDividerModule } from '@angular/material/divider';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatTableModule } from '@angular/material/table';
import { MatListModule } from '@angular/material/list';
import { MatMenuModule } from '@angular/material/menu';
import { ReactiveFormsModule } from '@angular/forms';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { DragDropModule } from '@angular/cdk/drag-drop';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { CubejsClientModule, QueryBuilderService } from '@cubejs-client/ngx';
import { ChartsModule } from 'ng2-charts';
import { HttpLink } from 'apollo-angular/http';
import { APOLLO_OPTIONS } from 'apollo-angular';
import { GridsterModule } from 'angular-gridster2';

import { AppComponent } from './app.component';
import { ExploreComponent } from './explore/explore.component';
import { MembersGroupComponent } from './explore/members-group/members-group.component';
import { TimeGroupComponent } from './explore/time-group/time-group.component';
import { OrderComponent } from './explore/order/order.component';
import { PivotComponent } from './explore/pivot/pivot.component';
import { SettingsDialogComponent } from './settings-dialog/settings-dialog.component';
import {
  FilterGroupComponent,
  FilterComponent,
} from './explore/filter-group/filter-group.component';
import { AppRoutingModule } from './app-routing.module';
import { DashboardComponent } from './dashboard/dashboard.component';
import { AddToDashboardDialogComponent } from './explore/add-to-dashboard-dialog/add-to-dashboard-dialog.component';
import { QueryRendererComponent } from './explore/query-renderer/query-renderer.component';
import apolloClient from '../graphql/client';

const cubejsOptions = {
  token: 'environment.CUBEJS_API_TOKEN',
  options: {
    apiUrl: 'http://localhost:4000/cubejs-api/v1',
  },
};

export function cubejsClientFactory(http: HttpClient) {
  return () =>
    new Promise((resolve) => {
      setTimeout(() => resolve({ token: '100500' }), 2000);
    });
}

@NgModule({
  declarations: [
    AppComponent,
    ExploreComponent,
    DashboardComponent,
    QueryRendererComponent,
    MembersGroupComponent,
    TimeGroupComponent,
    OrderComponent,
    PivotComponent,
    SettingsDialogComponent,
    AddToDashboardDialogComponent,
    FilterGroupComponent,
    FilterComponent,
  ],
  entryComponents: [SettingsDialogComponent, AddToDashboardDialogComponent],
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
    DragDropModule,
    MatButtonToggleModule,
    MatTabsModule,
    MatTableModule,
    MatInputModule,
    MatCheckboxModule,
    MatDialogModule,
    MatSnackBarModule,
    ReactiveFormsModule,
    MatListModule,
    AppRoutingModule,
    MatMenuModule,
    GridsterModule,
  ],
  providers: [
    QueryBuilderService,
    {
      provide: APOLLO_OPTIONS,
      useFactory: () => apolloClient,
      deps: [HttpLink],
    },
    {
      provide: APP_INITIALIZER,
      useFactory: cubejsClientFactory,
      deps: [HttpClient],
      multi: true,
    },
  ],
  bootstrap: [AppComponent],
})
export class AppModule {
  constructor() {}
}
