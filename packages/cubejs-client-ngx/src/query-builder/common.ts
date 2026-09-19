import { BehaviorSubject } from 'rxjs';

export class StateSubject<T = any> {
  public subject: BehaviorSubject<T>;

  public constructor(value: T) {
    this.subject = new BehaviorSubject(value);
  }

  public get() {
    return this.subject.getValue();
  }

  public set(value: T) {
    this.subject.next(value);
  }
}
