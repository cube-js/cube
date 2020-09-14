import { BehaviorSubject } from 'rxjs';

export abstract class StateSubject<T = any> {
  subject: BehaviorSubject<T>;

  constructor(value: T) {
    this.subject = new BehaviorSubject(value);
  }
}
