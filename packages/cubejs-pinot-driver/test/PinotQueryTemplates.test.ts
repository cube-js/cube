import { PinotQuery } from '../src/PinotQuery';

describe('PinotQuery SQL templates', () => {
  it('supports Tesseract select template variables', () => {
    const templates = PinotQuery.prototype.sqlTemplates.call({} as PinotQuery);
    const selectTemplate = templates.statements.select;

    expect(selectTemplate).toContain('from_prepared');
    expect(selectTemplate).toContain('ctes');
    expect(selectTemplate).toContain('distinct');
    expect(selectTemplate).toContain('joins');
    expect(selectTemplate).toContain('filter');
    expect(selectTemplate).toContain('having');
    expect(selectTemplate.indexOf('OFFSET')).toBeLessThan(selectTemplate.indexOf('LIMIT'));
  });
});
