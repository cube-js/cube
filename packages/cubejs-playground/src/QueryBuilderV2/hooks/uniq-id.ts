import { useMemo } from 'react';
import { nanoid } from 'nanoid/non-secure';

/**
 * @param length {number} length of hash string
 * @param prefix {string} prefix before hash
 */
export const useUniqID = ({ length = 5, prefix = 'id' } = {}) =>
  useMemo(() => `${prefix}_${nanoid(length)}`, [prefix, length]);
