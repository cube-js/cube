import { BrowserModule } from "@angular/platform-browser";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { NgModule } from "@angular/core";
import { CubejsClientModule } from "@cubejs-client/ngx";
import { MatCardModule } from "@angular/material/card";

import { AppComponent } from "./app.component";
import { ChartsModule } from "ng2-charts";
import { BarChartComponent } from "./bar-chart/bar-chart.component";
import { MatGridListModule } from "@angular/material/grid-list";
import { MatMenuModule } from "@angular/material/menu";
import { MatIconModule } from "@angular/material/icon";
import { MatButtonModule } from "@angular/material/button";
import { MatProgressBarModule } from "@angular/material/progress-bar";
import { LayoutModule } from "@angular/cdk/layout";
import { DashboardPageComponent } from "./dashboard-page/dashboard-page.component";
import { KpiCardComponent } from "./kpi-card/kpi-card.component";
import { CountUpModule } from "ngx-countup";
import { DoughnutChartComponent } from "./doughnut-chart/doughnut-chart.component";
import { AppRoutingModule } from "./app-routing.module";
import { MatListModule } from "@angular/material/list";
import { TablePageComponent } from "./table-page/table-page.component";
import { MaterialTableComponent } from "./material-table/material-table.component";
import { MatTableModule } from "@angular/material/table";
import { MatPaginatorModule } from "@angular/material/paginator";
import { MatProgressSpinnerModule } from "@angular/material/progress-spinner";
import { TableFiltersComponent } from "./table-filters/table-filters.component";
import { MatButtonToggleModule } from "@angular/material/button-toggle";
import { MatDatepickerModule } from "@angular/material/datepicker";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatNativeDateModule } from "@angular/material/core";
import { MatInputModule } from "@angular/material/input";
import {MatSliderModule} from "@angular/material/slider";
import { UserAvatarComponent } from './user-avatar/user-avatar.component';
import { environment } from 'src/environments/environment';

const API_URL = environment.apiUrl;
const cubejsOptions = {
  token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1Ijp7fSwiaWF0IjoxNjAzNzMzMDA2fQ.zZ6C1OqWFuF0od3eb_M6Fqzeb7a8g4JXYepIes3CLos",
  options: { apiUrl: `${API_URL}/cubejs-api/v1` }
};

@NgModule({
  declarations: [
    AppComponent,
    BarChartComponent,
    DashboardPageComponent,
    KpiCardComponent,
    DoughnutChartComponent,
    TablePageComponent,
    MaterialTableComponent,
    TableFiltersComponent,
    UserAvatarComponent
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
    LayoutModule,
    CountUpModule,
    MatProgressBarModule,
    AppRoutingModule,
    MatListModule,
    MatTableModule,
    MatPaginatorModule,
    MatProgressSpinnerModule,
    MatButtonToggleModule,
    MatDatepickerModule,
    MatFormFieldModule,
    MatNativeDateModule,
    MatInputModule,
    MatSliderModule
  ],
  providers: [MatDatepickerModule],
  bootstrap: [AppComponent]
})
export class AppModule {
}
