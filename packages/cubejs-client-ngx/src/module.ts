import { NgModule, ModuleWithProviders } from '@angular/core';
import { CubejsClient } from './client';

@NgModule({
  providers: [CubejsClient],
})
export class CubejsClientModule {
  public static forRoot(config: any): ModuleWithProviders<CubejsClientModule> {
    return {
      ngModule: CubejsClientModule,
      providers: [
        CubejsClient,
        {
          provide: 'config',
          useValue: config,
        },
      ],
    };
  }
}
