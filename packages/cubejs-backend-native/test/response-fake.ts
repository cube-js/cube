import stream from 'stream';

const RANDOM_AGENTS = [
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36 RuxitSynthetic/1.0 v3641179851656494467 t7257912775283346076 ath5ee645e0 altpriv cvcv=2 smf=0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36 RuxitSynthetic/1.0 v4563706913765908914 t4763100215355965436 ath259cea6f altpriv cvcv=2 smf=0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 RuxitSynthetic/1.0 v3366394972 t3176537711211975202 athfa3c3975 altpub cvcv=2 smf=0',
  'Mozilla/5.0 (Linux; arm_64; Android 5.0.2; ASUS_Z00UD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 YaBrowser/19.9.4.104.00 Mobile Safari/537.36',
  'Mozilla/5.0 (Linux; Android 9; SM-J600G) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Mobile Safari/537.36',
  'Mozilla/5.0 (Linux; arm_64; Android 7.0; K6000 Plus) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 YaBrowser/20.2.2.127.00 Yptp/1.62 Mobile Safari/537.36'
];

type RowGenerator = (id: number) => string;

function createRowGenerator(query: any, total: number) {
  if (query.measures && query.measures.length) {
    throw Error('measures are not supported');
  }

  if (query.segments && query.measures.segments) {
    throw Error('segments are not supported');
  }

  const period = Math.min(365 * 5, total);

  let startTime = new Date().getTime() - (period * 60 * 60 * 24 * 1000);
  let byDay = (period / total) * 86400 * 1000;

  if (query.order) {
    if (query.order.length > 1) {
      throw Error('Unable to use multiple columns in orders');
    }

    const [orderKey, orderMode] = query.order[0];

    if (['Logs.timestamp', 'KibanaSampleDataEcommerce.order_date'].includes(orderKey) && orderMode.toLowerCase() === 'desc') {
      startTime = new Date().getTime();
      byDay *= -1;
    }
  }

  function dateGenerator(id: number) {
    const d = new Date();
    d.setTime(startTime + (byDay * id));

    return d.toISOString().substring(0, 23);
  }

  const generators: Record<string, RowGenerator> = {
    'KibanaSampleDataEcommerce.taxful_total_price': (_id: number) => (Math.random() * 100).toString(),
    'KibanaSampleDataEcommerce.customer_gender': (id: number) => (id % 2 ? 'male' : 'female'),
    'KibanaSampleDataEcommerce.order_date': dateGenerator,
    'KibanaSampleDataEcommerce.id': (id) => id.toString(),
    'Logs.timestamp': dateGenerator,
    'Logs.agent': (id: number) => RANDOM_AGENTS[id % RANDOM_AGENTS.length],
  };

  const gen: [string, RowGenerator][] = [];

  if (query.dimensions) {
    for (const dimension of query.dimensions) {
      if (generators[dimension]) {
        gen.push([dimension, generators[dimension]]);
      } else {
        throw new Error(`Unable to generate fake value for dimension called: ${dimension}`);
      }
    }
  }

  return (i: number) => {
    const res: Record<string, string> = {};

    for (const [key, val] of gen) {
      res[key] = val(i);
    }

    return res;
  };
}

export class FakeRowStream extends stream.Readable {
  protected readonly genNext: (_i: number) => Record<string, string>;

  protected readonly limit: number;

  protected total: number = 0;

  public constructor(query: any) {
    super({
      objectMode: true,
      highWaterMark: 1024,
    });
    this.limit = query.limit || 100;
    this.genNext = createRowGenerator(query, this.limit);
    this.setMaxListeners(10);
  }

  public _read(size: number) {
    let toFetch = size;

    if (this.total + size > this.limit) {
      toFetch = this.limit - this.total;
    }

    const endRange = this.total + toFetch;

    for (; this.total < endRange; this.total++) {
      this.push(this.genNext(this.total));
    }

    if (this.total >= this.limit) {
      this.push(null);
    }
  }
}
