import { Injectable } from '@angular/core';
import { BehaviorSubject, from, pipe } from 'rxjs';

const wait = (delay = 200) =>
  new Promise((resolve) => setTimeout(resolve, delay));

@Injectable()
export class AuthService {
  public token$ = new BehaviorSubject<string | null>(null);
  public token = null;

  constructor() {}

  get isAuthorized() {
    return Boolean(this.token);
  }

  login(userName: string, password: string) {
    const authPromise = new Promise((resolve) =>
      setTimeout(() => {
        this.token$.next(`${userName}:${Math.random().toString()}`);
        resolve(`${userName}:${Math.random().toString()}`);
      }, 1000)
    );
    return from(authPromise);
  }

  logout() {
    this.token$.next(null);
  }
}
