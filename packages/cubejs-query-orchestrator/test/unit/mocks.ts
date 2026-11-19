import type { BaseDriver } from '@cubejs-backend/base-driver';
import type { DriverFactory } from '../../src';

/**
 * Test doubles implement only the part of an interface the code under test touches. These
 * helpers are the one place where such a partial object is passed off as the full type.
 */
export const partialMock = <T extends object>(mock: Partial<T>): T => mock as T;

export const asDriver = <T = BaseDriver>(mock: object): T => mock as unknown as T;

export const unusedDriverFactory: DriverFactory = async () => {
  throw new Error('This test is not expected to reach the driver');
};
