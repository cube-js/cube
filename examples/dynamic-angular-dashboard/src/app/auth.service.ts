import { Injectable } from '@angular/core';
import { from, pipe } from 'rxjs';

const wait = (delay = 2000) =>
  new Promise((resolve) => setTimeout(resolve, delay));

@Injectable()
export class AuthService {
  public token: string | null = null;

  constructor() {}

  get isAuthorized() {
    return Boolean(this.token);
  }

  login(userName: string, password: string) {
    const authPromise = new Promise((resolve) =>
      setTimeout(() => {
        this.token = `${userName}:${Math.random().toString()}`;
        resolve(`${userName}:${Math.random().toString()}`);
      }, 2000)
    );
    return from(authPromise);
  }

  logout() {
    this.token = null;
  }
}
