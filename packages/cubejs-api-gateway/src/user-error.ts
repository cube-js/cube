import { CubejsHandlerError } from './cubejs-handler-error';

export class UserError extends CubejsHandlerError {
  public constructor(message: string) {
    super(400, 'User Error', message);
  }
}
