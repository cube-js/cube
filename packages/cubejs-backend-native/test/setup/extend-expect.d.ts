declare namespace jest {
  interface Expect {
    toBeTypeOrNull: any
  }
  interface Matchers<R, T = {}> {
    toBeTypeOrNull(expected: any): R;
  }
}