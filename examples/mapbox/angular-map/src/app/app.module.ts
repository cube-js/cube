import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppComponent } from './app.component';
import { FormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NZ_I18N } from 'ng-zorro-antd/i18n';
import { en_US } from 'ng-zorro-antd/i18n';
import { registerLocaleData } from '@angular/common';
import { CubejsClientModule } from '@cubejs-client/ngx';
import en from '@angular/common/locales/en';

import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzLayoutModule } from 'ng-zorro-antd/layout';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzSliderModule } from 'ng-zorro-antd/slider';
import { ChoroplethComponent } from './choropleth/choropleth.component';

const cubejsOptions = {
  token:
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1ODY2MTg3NDcsImV4cCI6MTU4NjcwNTE0N30.1M3LWja51cQJ8Hgoja8joBU-Z9o6vbhtqnV72WsTAic',
  options: {
    apiUrl: 'https://react-query-builder.herokuapp.com/cubejs-api/v1'
  }
};

registerLocaleData(en);

@NgModule({
  declarations: [AppComponent, ChoroplethComponent],
  imports: [
    BrowserModule,
    FormsModule,
    HttpClientModule,
    BrowserAnimationsModule,
    CubejsClientModule.forRoot(cubejsOptions),
    NzGridModule,
    NzLayoutModule,
    NzCardModule,
    NzDividerModule,
    NzIconModule,
    NzTabsModule,
    NzSliderModule
  ],
  providers: [{ provide: NZ_I18N, useValue: en_US }],
  bootstrap: [AppComponent]
})
export class AppModule {}
