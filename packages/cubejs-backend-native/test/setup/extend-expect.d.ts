declare namespace jest {
  interface Expect {
    toBeTypeOrNull: any
  }
  interface Matchers<R, _T = {}> {
    toBeTypeOrNull(expected: any): R;
  }
}
