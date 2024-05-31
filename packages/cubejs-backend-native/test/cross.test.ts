import { loadNative, setupLogger } from '../js';

setupLogger(({ event }) => console.log(event), 'trace');

describe('Cross language representation (CLR)', () => {
  it('all types', async () => {
    const native = loadNative();

    const clr: any = {
      true_field: true,
      false_field: false,
      null_field: null,
      array_of_objects: [{
        title: 'object 1',
      }, {
        title: 'object 2',
      }]
    };

    expect(native.__js_to_clrepr_to_js(clr)).toEqual({
      true_field: true,
      false_field: false,
      null_field: undefined,
      array_of_objects: [{
        title: 'object 1',
      }, {
        title: 'object 2',
      }]
    });
  });

  it('circular referenced object', async () => {
    const native = loadNative();

    function CircularReferencedObject(): any {
      // @ts-ignore
      this.abc = 'Crazy developers are here';
      // @ts-ignore
      this.circular = this;
    }

    // @ts-ignore
    const input = new CircularReferencedObject();

    expect(native.__js_to_clrepr_to_js(input)).toEqual({
      abc: 'Crazy developers are here',
    });
  });

  it('array of circular referenced objectes', async () => {
    const native = loadNative();

    function CircularReferencedObject(): any {
      // @ts-ignore
      this.abc = 'Crazy developers are here';
      // @ts-ignore
      this.circular = this;
    }

    // @ts-ignore
    const element1 = new CircularReferencedObject();
    // @ts-ignore
    const element2 = new CircularReferencedObject();

    expect(native.__js_to_clrepr_to_js([element1, element2])).toEqual([
      {
        abc: 'Crazy developers are here',
      },
      {
        abc: 'Crazy developers are here',
      }
    ]);
  });

  it('circular referenced array', async () => {
    const native = loadNative();

    class MyCrazyArray extends Array {
      public constructor() {
        super();

        this.push(this);
      }

      public get [Symbol.species]() {
        return Array;
      }
    }

    expect(native.__js_to_clrepr_to_js(new MyCrazyArray())).toEqual([]);
  });
});
