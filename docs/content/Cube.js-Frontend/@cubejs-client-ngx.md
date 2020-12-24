---
title: '@cubejs-client/ngx'
permalink: /@cubejs-client-ngx
category: Cube.js Frontend
subCategory: Reference
menuOrder: 4
---

`@cubejs-client/ngx` provides Angular Module for easy integration Cube.js
into an Angular app.

## Installation

First, install `@cubejs-client/ngx` using npm or yarn:

```bash
$ npm install --save @cubejs-client/ngx
# or
$ yarn add @cubejs-client/ngx
```

Now you can add `CubejsClientModule` to your **app.module.ts** file:

```typescript
import { CubejsClientModule } from '@cubejs-client/ngx';
import { environment } from '../../environments/environment';

const cubejsOptions = {
  token: environment.CUBEJS_API_TOKEN,
  options: { apiUrl: environment.CUBEJS_API_URL }
};

@NgModule({
  declarations: [
    ...
  ],
  imports: [
    ...,
    CubejsClientModule.forRoot(cubejsOptions)
  ],
  providers: [...],
  bootstrap: [...]
})
export class AppModule { }
```

The `options` object is passed directly to [@cubejs-client/core](/@cubejs-client-core).

`CubejsClientModule` provides `CubejsClient`, which you can inject into your components or services:

```typescript
import { CubejsClient } from '@cubejs-client/ngx';

export class AppComponent {
  constructor(private cubejs:CubejsClient){}

  ngOnInit(){
    this.cubejs.load({
      measures: ["some_measure"]
    }).subscribe(
      resultSet => {
        this.data = resultSet.chartPivot();
      },
      err => console.log('HTTP Error', err)
    );
  }
}
```

## API

`CubejsClient` provides the same API methods as [@cubejs-client/core](/@cubejs-client-core#cubejs-api).
The difference is that instead of Promise it returns an [Observable](http://reactivex.io/rxjs/class/es6/Observable.js~Observable.html),
which passes [resultSet](/@cubejs-client-core#result-set) into callback function.

Also you can use [RxJS Subject](https://rxjs-dev.firebaseapp.com/guide/subject) with a query using `watch` method:

```typescript
private query;

ngOnInit() {
  this.query = new Subject();
  this.cubejs.watch(this.query).subscribe(
    resultSet => {
      console.log(resultSet.chartPivot()[0].x);
      console.log(resultSet.seriesNames()[0]);
    },
    err => console.log('HTTP Error', err)
  );
}

button1ClickHandler() {
  this.query.next({ query_1 });
}

button2ClickHandler() {
  this.query.next({ query_2 });
}
```

