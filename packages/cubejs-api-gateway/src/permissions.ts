import { Permission } from './types/strings';
import { PermissionsTuple } from './types/auth';
import { CubejsHandlerError } from './CubejsHandlerError';

/**
 * Returns default permissions list (all default permissions).
 */
export const defaultSystemPermissions = [
  'liveliness',
  'graphql',
  'meta',
  'data',
  'jobs'
];

/**
 * Assert permission.
 *
 * @throw CubejsHandlerError
 */
export function assertPermission(
  permission: Permission,
  config: {
    allow: PermissionsTuple,
    deny: PermissionsTuple,
  },
) {
  let allowed = false;
  let denied = false;
  config.allow.forEach((p) => {
    allowed = allowed || permission === p;
  });
  config.deny.forEach((p) => {
    denied = denied || permission === p;
  });
  if (!allowed && !denied) {
    throw new CubejsHandlerError(
      403,
      'Forbidden',
      `The specified '${
        permission
      }' permission is missing in the config: ${
        JSON.stringify(config, undefined, 2)
      }`
    );
  }
  if (!allowed || denied) {
    throw new CubejsHandlerError(
      403,
      'Forbidden', `You don't have the '${permission}' permission.`
    );
  }
}
