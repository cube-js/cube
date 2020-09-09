import { Injectable } from '@angular/core';

@Injectable()
export class Builder {
  constructor() {
    console.log(new Date());
  }
}