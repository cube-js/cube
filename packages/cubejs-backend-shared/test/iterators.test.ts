test('for of', () => {
  const xs = [];
  for (const x of ['one', 'two', 'three']) {
    xs.push(x);
  }
  expect(xs).toEqual(['one', 'two', 'three']);
});

test('for of keys', () => {
  const xs = [];
  for (const x of Object.keys({ one: 1, two: 2, three: 3 })) {
    xs.push(x);
  }
  expect(xs).toEqual(['one', 'two', 'three']);
});

async function* gen() {
  yield 'one';
  yield 'two';
  yield 'three';
}

test('for await', async () => {
  const xs = [];
  for await (const x of gen()) {
    xs.push(x);
  }
  expect(xs).toEqual(['one', 'two', 'three']);
});
