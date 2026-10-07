import { TestBed } from '@angular/core/testing';
import { BehaviorSubject, Subject } from 'rxjs';

import { CubeClient } from '../src/client';
import { CubeClientModule } from '../src/module';

describe('CubeClientModule', () => {
  afterEach(() => {
    TestBed.resetTestingModule();
  });

  test('forRoot provides CubeClient with the given config', () => {
    TestBed.configureTestingModule({
      imports: [
        CubeClientModule.forRoot({
          token: 'token',
          options: { apiUrl: 'http://localhost:4000/cubejs-api/v1' },
        }),
      ],
    });

    const client = TestBed.inject(CubeClient);

    expect(client).toBeInstanceOf(CubeClient);
    expect(client.ready$.getValue()).toBe(true);
  });

  test('an observable config marks the client ready only once it emits', () => {
    const config = new Subject<any>();

    TestBed.configureTestingModule({
      imports: [CubeClientModule.forRoot(config)],
    });

    const client = TestBed.inject(CubeClient);

    expect(client.ready$.getValue()).toBe(false);

    config.next({ token: 'token', options: {} });

    expect(client.ready$.getValue()).toBe(true);
  });

  test('a BehaviorSubject config is ready right away', () => {
    TestBed.configureTestingModule({
      imports: [
        CubeClientModule.forRoot(
          new BehaviorSubject({ token: 'token', options: {} })
        ),
      ],
    });

    expect(TestBed.inject(CubeClient).ready$.getValue()).toBe(true);
  });
});
