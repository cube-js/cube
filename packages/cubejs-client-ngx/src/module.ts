import { NgModule, ModuleWithProviders } from '@angular/core';
import { CubeClient } from './client';

@NgModule({
  providers: [CubeClient],
})
export class CubeClientModule {
  public static forRoot(config: any): ModuleWithProviders<CubeClientModule> {
    return {
      ngModule: CubeClientModule,
      providers: [
        CubeClient,
        {
          provide: 'config',
          useValue: config,
        },
      ],
    };
  }
}
