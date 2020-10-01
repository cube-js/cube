import { BehaviorSubject } from 'rxjs';

export class StateSubject<T = any> {
  subject: BehaviorSubject<T>;

  constructor(value: T) {
    this.subject = new BehaviorSubject(value);
  }

  get() {
    return this.subject.getValue();
  }

  set(value: T) {
    this.subject.next(value);
  }
}
