import { Inject, Injectable } from '@angular/core';
import { BehaviorSubject, Subject } from 'rxjs';

// @Injectable()
export class CubejsService {
  public token: any = null;
  // apiInstance$ = new BehaviorSubject<any>(null);
  // config$ = new BehaviorSubject<any>(null);

  constructor(public token$: Subject<string | null>) {
    this.token$.subscribe((token) => (this.token = token));
  }
  // constructor(public options: any, public token$: Subject<string | null>) {
  // console.log('CubejsService.constructor', Date.now());
  // }

  // getToken() {
  // return this.token;
  // }
}
