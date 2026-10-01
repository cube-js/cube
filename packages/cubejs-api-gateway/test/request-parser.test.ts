import { getRequestIdFromRequest } from '../src/request-parser';
import { UserError } from '../src/user-error';

const requestWithHeaders = (headers: Record<string, string>): any => ({
  get: (name: string) => headers[name],
});

describe('getRequestIdFromRequest', () => {
  it('uses x-request-id, then traceparent', () => {
    expect(getRequestIdFromRequest(requestWithHeaders({ 'x-request-id': 'req-1', traceparent: 'tp' }))).toBe('req-1');
    expect(getRequestIdFromRequest(requestWithHeaders({ traceparent: 'tp-1' }))).toBe('tp-1');
  });

  it('rejects an id with forbidden characters', () => {
    expect(() => getRequestIdFromRequest(requestWithHeaders({ 'x-request-id': 'my req' }))).toThrow(UserError);
    expect(() => getRequestIdFromRequest(requestWithHeaders({ traceparent: 'café' }))).toThrow(UserError);
    expect(() => getRequestIdFromRequest(requestWithHeaders({ 'x-request-id': "req'1" }))).toThrow(UserError);
    expect(() => getRequestIdFromRequest(requestWithHeaders({ 'x-request-id': 'a'.repeat(129) }))).toThrow(UserError);
  });

  it('generates an id when none is supplied', () => {
    expect(getRequestIdFromRequest(requestWithHeaders({}))).toMatch(/^[0-9a-f-]{36}-span-1$/);
  });
});
