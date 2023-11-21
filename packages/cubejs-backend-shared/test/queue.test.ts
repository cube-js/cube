import { AbstractSetMemoryQueue, pausePromise } from '../src';

describe('AbstractSetMemoryQueue', () => {
  it('concurrency(1)', async () => {
    jest.setTimeout(10 * 1000);

    let capacityFullWarnings = 0;
    const executed: string[] = [];

    class TestQueue extends AbstractSetMemoryQueue {
      protected async execute(item: string): Promise<void> {
        executed.push(item);

        await pausePromise(15);
      }

      protected onCapacity(): void {
        capacityFullWarnings++;
      }
    }

    const all = [];

    const queue = new TestQueue(
      5,
      1
    );

    all.push(queue.addToQueue('a'));
    all.push(queue.addToQueue('ab')); all.push(queue.addToQueue('ab'));
    all.push(queue.addToQueue('abc'));
    all.push(queue.addToQueue('abcd'));
    all.push(queue.addToQueue('abcde')); all.push(queue.addToQueue('abcde'));
    all.push(queue.addToQueue('abcdef'));
    all.push(queue.addToQueue('abcdefg'));
    all.push(queue.addToQueue('abcdefgh'));
    all.push(queue.addToQueue('abcdefghk')); all.push(queue.addToQueue('abcdefghk'));
    all.push(queue.addToQueue('abcdefghkl'));
    all.push(queue.addToQueue('abcdefghklm'));
    all.push(queue.addToQueue('abcdefghklmn')); all.push(queue.addToQueue('abcdefghklmn'));
    all.push(queue.addToQueue('abcdefghklmno'));
    all.push(queue.addToQueue('abcdefghklmnop'));
    all.push(queue.addToQueue('abcdefghklmnopw'));

    await Promise.all(all);

    expect(executed.length).toEqual(15);
    expect(capacityFullWarnings).toBeGreaterThan(10);
    expect(executed).toEqual([
      'a',
      'ab',
      'abc',
      'abcd',
      'abcde',
      'abcdef',
      'abcdefg',
      'abcdefgh',
      'abcdefghk',
      'abcdefghkl',
      'abcdefghklm',
      'abcdefghklmn',
      'abcdefghklmno',
      'abcdefghklmnop',
      'abcdefghklmnopw',
    ]);
  });
});
