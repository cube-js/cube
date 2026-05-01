import fs from 'fs';
import path from 'path';

import { prepareYamlCompiler } from './PrepareCompiler';

describe('Format showcase fixture', () => {
  let metaTransformer: any;

  beforeAll(async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/format_showcase.yml'),
      'utf8'
    );
    const compilers = prepareYamlCompiler(modelContent);
    await compilers.compiler.compile();
    metaTransformer = compilers.metaTransformer;
  });

  const findMeasure = (cubeName: string, measureName: string) => {
    const cubeConfig = metaTransformer.cubes
      .map((def: any) => def.config)
      .find((def: any) => def.name === cubeName);
    expect(cubeConfig).toBeDefined();
    const measure = cubeConfig.measures.find((m: any) => m.name === `${cubeName}.${measureName}`);
    expect(measure).toBeDefined();

    return measure;
  };

  const expectCurrencyMeasure = (measure: any, currency: string | undefined) => {
    expect(measure.format).toBe('currency');
    expect(measure.currency).toBe(currency);
    const expectedDescription: any = { name: 'currency', specifier: '$,.2~f' };
    if (currency) {
      expectedDescription.currency = currency;
    }
    expect(measure.formatDescription).toEqual(expectedDescription);
  };

  describe('currency measures on cube', () => {
    it('revenue_usd has formatDescription with currency USD', () => {
      expectCurrencyMeasure(findMeasure('format_showcase', 'revenue_usd'), 'USD');
    });

    it('revenue_eur has formatDescription with currency EUR', () => {
      expectCurrencyMeasure(findMeasure('format_showcase', 'revenue_eur'), 'EUR');
    });

    it('revenue_rub has formatDescription with currency RUB', () => {
      expectCurrencyMeasure(findMeasure('format_showcase', 'revenue_rub'), 'RUB');
    });

    it('revenue_default has formatDescription without currency field when none specified', () => {
      expectCurrencyMeasure(findMeasure('format_showcase', 'revenue_default'), undefined);
    });
  });

  describe('currency measures on view', () => {
    it('total_sales_usd (aliased) preserves full formatDescription with currency USD', () => {
      expectCurrencyMeasure(findMeasure('format_showcase_view', 'total_sales_usd'), 'USD');
    });

    it('total_sales_eur (aliased) preserves full formatDescription with currency EUR', () => {
      expectCurrencyMeasure(findMeasure('format_showcase_view', 'total_sales_eur'), 'EUR');
    });

    it('revenue_rub (not aliased) preserves full formatDescription with currency RUB', () => {
      expectCurrencyMeasure(findMeasure('format_showcase_view', 'revenue_rub'), 'RUB');
    });

    it('default_currency_revenue (aliased) has no currency field when none specified', () => {
      expectCurrencyMeasure(findMeasure('format_showcase_view', 'default_currency_revenue'), undefined);
    });
  });
});
