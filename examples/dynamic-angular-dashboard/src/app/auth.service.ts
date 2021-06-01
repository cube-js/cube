import { Injectable } from '@angular/core';
import { BehaviorSubject, from } from 'rxjs';
import { CubejsConfig } from '@cubejs-client/ngx';

import { cubejsConfig } from './app.module';

@Injectable()
export class AuthService {
  public config$ = new BehaviorSubject<CubejsConfig | null>(null);

  constructor() {
    this.login().then(() => console.log('config ready'));
  }

  // Used as an example to show async CubejsClient initialization
  public login(): any {
    return new Promise<void>((resolve) =>
      setTimeout(() => {
        this.config$.next({
          token: cubejsConfig.token,
          options: {
            apiUrl: cubejsConfig.options.apiUrl,
          },
        });
        resolve();
      }, 0)
    );
  }

  public logout(): void {
    this.config$.next(null);
  }
}
