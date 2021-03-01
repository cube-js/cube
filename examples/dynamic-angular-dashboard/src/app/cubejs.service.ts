import { Inject, Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

@Injectable()
export class CubejsService {
  apiInstance$ = new BehaviorSubject<any>(null);

  config$ = new BehaviorSubject<any>(null);

  constructor(public options: any, public token: string | null) {
    console.log('CubejsService.constructor', Date.now());
  }
}
