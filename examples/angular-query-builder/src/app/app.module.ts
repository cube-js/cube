import { HttpClientModule } from '@angular/common/http';
import { NgModule } from '@angular/core';
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
import { ReactiveFormsModule } from '@angular/forms';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { DragDropModule } from '@angular/cdk/drag-drop';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { CubejsClientModule, QueryBuilderService } from '@cubejs-client/ngx';
import { ChartsModule } from 'ng2-charts';

import { AppComponent } from './app.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { QueryRendererComponent } from './dashboard/query-renderer/query-renderer.component';
import { MembersGroupComponent } from './dashboard/members-group/members-group.component';
import { TimeGroupComponent } from './dashboard/time-group/time-group.component';
import { OrderComponent } from './dashboard/order/order.component';
import { PivotComponent } from './dashboard/pivot/pivot.component';
import { SettingsDialogComponent } from './settings-dialog/settings-dialog.component';
import {
  FilterGroupComponent,
  FilterComponent,
} from './dashboard/filter-group/filter-group.component';

const cubejsOptions = {
  token: 'environment.CUBEJS_API_TOKEN',
  options: {
    apiUrl: 'http://localhost:4000/cubejs-api/v1',
  },
};

@NgModule({
  declarations: [
    AppComponent,
    DashboardComponent,
    QueryRendererComponent,
    MembersGroupComponent,
    TimeGroupComponent,
    OrderComponent,
    PivotComponent,
    SettingsDialogComponent,
    FilterGroupComponent,
    FilterComponent,
  ],
  entryComponents: [SettingsDialogComponent],
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
    ReactiveFormsModule
  ],
  providers: [QueryBuilderService],
  bootstrap: [AppComponent],
})
export class AppModule {
  constructor() {}
}
