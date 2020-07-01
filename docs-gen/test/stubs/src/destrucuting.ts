/**
 * Destructuring objects.
 */
const { destructObjectA, destructObjectB, destructObjectC } = {
  destructObjectA: 0,
  destructObjectB: 'string',
  destructObjectC: 0,
};

/**
 * Destructuring arrays.
 */
const [destructArrayA, destructArrayB, destructArrayC = 10] = [0, 'string', 0];

/**
 * Array Destructuring with rest
 */
const [destructArrayWithRestA, destructArrayWithRestB, ...destructArrayWithRest] = [1, 2, 3, 4];

/**
 * Array Destructuring with ignores
 */
const [destructArrayWithIgnoresA, , ...destructArrayWithIgnoresRest] = [1, 2, 3, 4];

/**
 * Destructuring function parameters.
 */
function drawText({ text = '', location: [x, y] = [0, 0], bold = false }) {}

interface I {
  name: string;
}
