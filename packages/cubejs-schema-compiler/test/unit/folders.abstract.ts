import { CubeToMetaTransformer } from 'src/compiler/CubeToMetaTransformer';

type PreparedCompiler = {
  compiler: any;
  metaTransformer: CubeToMetaTransformer;
};

export function runFoldersTestSuite(
  suiteName: string,
  prepareMainFixture: () => PreparedCompiler,
  prepareFlattenedFixture?: () => PreparedCompiler
) {
  describe(suiteName, () => {
    let metaTransformer: CubeToMetaTransformer;
    let compiler: any;

    beforeAll(async () => {
      const prepared = prepareMainFixture();
      compiler = prepared.compiler;
      metaTransformer = prepared.metaTransformer;

      await compiler.compile();
    });

    it('a folder with includes all and named members', async () => {
      const emptyView = metaTransformer.cubes.find(
        (it) => it.config.name === 'test_view'
      );

      expect(emptyView?.config.folders.length).toBe(2);

      const folder1 = emptyView?.config.folders.find(
        (it) => it.name === 'folder1'
      );
      expect(folder1?.members).toEqual([
        'test_view.age',
        'test_view.renamed_gender',
      ]);

      const folder2 = emptyView?.config.folders.find(
        (it) => it.name === 'folder2'
      );
      expect(folder2?.members).toEqual(
        expect.arrayContaining(['test_view.age', 'test_view.renamed_gender'])
      );
    });

    it('a nested folders with some * and named members (merged)', async () => {
      const testView = metaTransformer.cubes.find(
        (it) => it.config.name === 'test_view4'
      );

      expect(testView?.config.folders.length).toBe(3);

      const folder1 = testView?.config.folders.find(
        (it) => it.name === 'folder1'
      );
      expect(folder1?.members).toEqual([
        'test_view4.users_age',
        'test_view4.users_state',
        'test_view4.renamed_orders_status',
      ]);

      const folder2 = testView?.config.folders.find(
        (it) => it.name === 'folder2'
      );
      expect(folder2?.members).toEqual(
        expect.arrayContaining(['test_view4.users_city', 'test_view4.users_renamed_in_view3_gender'])
      );

      const folder3 = testView?.config.folders.find(
        (it) => it.name === 'folder3'
      );
      expect(folder3?.members.length).toBe(9);
      expect(folder3?.members).toEqual([
        'test_view4.users_city',
        'test_view4.renamed_orders_status',
        'test_view4.renamed_orders_count',
        'test_view4.renamed_orders_id',
        'test_view4.renamed_orders_number',
        'test_view4.users_age',
        'test_view4.users_state',
        'test_view4.users_gender',
        'test_view4.users_renamed_in_view3_gender',
      ]);
    });

    it('a nested folders with some * and named members (flattened)', async () => {
      const originalDelimiter = process.env.CUBEJS_NESTED_FOLDERS_DELIMITER;
      process.env.CUBEJS_NESTED_FOLDERS_DELIMITER = '/';

      try {
        const prepareFixture = prepareFlattenedFixture || prepareMainFixture;
        const prepared = prepareFixture();
        const compilerL = prepared.compiler;
        const metaTransformerL = prepared.metaTransformer;

        await compilerL.compile();

        const testView = metaTransformerL.cubes.find(
          (it) => it.config.name === 'test_view4'
        );

        expect(testView?.config.folders.length).toBe(5);

        const folder1 = testView?.config.folders.find(
          (it) => it.name === 'folder1'
        );
        expect(folder1?.members).toEqual([
          'test_view4.users_age',
          'test_view4.users_state',
          'test_view4.renamed_orders_status',
        ]);

        const folder2 = testView?.config.folders.find(
          (it) => it.name === 'folder2'
        );
        expect(folder2?.members).toEqual(
          expect.arrayContaining(['test_view4.users_city', 'test_view4.users_renamed_in_view3_gender'])
        );

        const folder3 = testView?.config.folders.find(
          (it) => it.name === 'folder3'
        );
        expect(folder3?.members.length).toBe(1);
        expect(folder3?.members).toEqual([
          'test_view4.users_city',
        ]);

        const folder4 = testView?.config.folders.find(
          (it) => it.name === 'folder3/inner folder 4'
        );
        expect(folder4?.members.length).toBe(1);
        expect(folder4?.members).toEqual(['test_view4.renamed_orders_status']);

        const folder5 = testView?.config.folders.find(
          (it) => it.name === 'folder3/inner folder 5'
        );
        expect(folder5?.members.length).toBe(9);
        expect(folder5?.members).toEqual([
          'test_view4.renamed_orders_count',
          'test_view4.renamed_orders_id',
          'test_view4.renamed_orders_number',
          'test_view4.renamed_orders_status',
          'test_view4.users_age',
          'test_view4.users_state',
          'test_view4.users_gender',
          'test_view4.users_city',
          'test_view4.users_renamed_in_view3_gender',
        ]);
      } finally {
        // Restore original environment variable
        if (originalDelimiter === undefined) {
          delete process.env.CUBEJS_NESTED_FOLDERS_DELIMITER;
        } else {
          process.env.CUBEJS_NESTED_FOLDERS_DELIMITER = originalDelimiter;
        }
      }
    });

    it('a nested folders with some * and named members (nested)', async () => {
      const testView = metaTransformer.cubes.find(
        (it) => it.config.name === 'test_view4'
      );

      expect(testView?.config.nestedFolders.length).toBe(3);

      const folder1 = testView?.config.nestedFolders.find(
        (it) => it.name === 'folder1'
      );
      expect(folder1?.members).toEqual([
        'test_view4.users_age',
        'test_view4.users_state',
        'test_view4.renamed_orders_status',
      ]);

      const folder2 = testView?.config.nestedFolders.find(
        (it) => it.name === 'folder2'
      );
      expect(folder2?.members).toEqual(
        expect.arrayContaining(['test_view4.users_city', 'test_view4.users_renamed_in_view3_gender'])
      );

      const folder3 = testView?.config.nestedFolders.find(
        (it) => it.name === 'folder3'
      );
      expect(folder3?.members.length).toBe(3);
      expect(folder3?.members[1]).toEqual(
        { name: 'inner folder 4', members: ['test_view4.renamed_orders_status'] }
      );
      expect((folder3?.members[2] as any)?.name).toEqual('inner folder 5');
      expect((folder3?.members[2] as any)?.members).toEqual([
        'test_view4.renamed_orders_count',
        'test_view4.renamed_orders_id',
        'test_view4.renamed_orders_number',
        'test_view4.renamed_orders_status',
        'test_view4.users_age',
        'test_view4.users_state',
        'test_view4.users_gender',
        'test_view4.users_city',
        'test_view4.users_renamed_in_view3_gender',
      ]);
    });

    it('folders from view extending other view', async () => {
      const view2 = metaTransformer.cubes.find(
        (it) => it.config.name === 'test_view2'
      );
      const view3 = metaTransformer.cubes.find(
        (it) => it.config.name === 'test_view3'
      );

      expect(view2?.config.folders.length).toBe(1);
      expect(view3?.config.folders.length).toBe(2);

      const folder1 = view2?.config.folders.find(
        (it) => it.name === 'folder1'
      );
      expect(folder1?.members).toEqual([
        'test_view2.users_age',
        'test_view2.users_state',
        'test_view2.renamed_orders_status',
      ]);

      const folder1v3 = view3?.config.folders.find(
        (it) => it.name === 'folder1'
      );
      expect(folder1v3?.members).toEqual([
        'test_view3.users_age',
        'test_view3.users_state',
        'test_view3.renamed_orders_status',
      ]);

      const folder2 = view3?.config.folders.find(
        (it) => it.name === 'folder2'
      );
      expect(folder2?.members).toEqual(
        expect.arrayContaining(['test_view3.users_city', 'test_view3.users_renamed_in_view3_gender'])
      );
    });

    it('a folder with aliased and prefixed cubes', async () => {
      const view = metaTransformer.cubes.find(
        (it) => it.config.name === 'test_view2'
      );

      expect(view?.config.folders.length).toBe(1);

      const folder1 = view?.config.folders.find((it) => it.name === 'folder1');
      expect(folder1?.members).toEqual([
        'test_view2.users_age',
        'test_view2.users_state',
        'test_view2.renamed_orders_status',
      ]);
    });

    it('folders with join_path syntax', async () => {
      const view = metaTransformer.cubes.find(
        (it) => it.config.name === 'test_view_join_path'
      );

      expect(view?.config.folders.length).toBe(4);

      const ordersFolder = view?.config.folders.find(
        (it) => it.name === 'Orders Folder'
      );
      expect(ordersFolder?.members).toEqual(
        expect.arrayContaining([
          'test_view_join_path.orders_count',
          'test_view_join_path.orders_id',
          'test_view_join_path.orders_number',
          'test_view_join_path.orders_status',
        ])
      );

      const usersFolder = view?.config.folders.find(
        (it) => it.name === 'Users Folder'
      );
      expect(usersFolder?.members).toEqual(
        expect.arrayContaining([
          'test_view_join_path.users_age',
          'test_view_join_path.users_state',
          'test_view_join_path.users_city',
          'test_view_join_path.users_gender',
        ])
      );

      const addressesFolder = view?.config.folders.find(
        (it) => it.name === 'Addresses Folder'
      );
      expect(addressesFolder?.members).toEqual(
        expect.arrayContaining([
          'test_view_join_path.addresses_street',
          'test_view_join_path.addresses_zip_code',
        ])
      );

      const mixedFolder = view?.config.folders.find(
        (it) => it.name === 'Mixed Folder'
      );
      expect(mixedFolder?.members).toEqual(
        expect.arrayContaining([
          // From users join_path
          'test_view_nested_join_path.users_age',
          'test_view_nested_join_path.users_state',
          'test_view_nested_join_path.users_city',
          'test_view_nested_join_path.users_gender',
          // From addresses join_path
          'test_view_nested_join_path.addresses_street',
          'test_view_nested_join_path.addresses_zip_code',
          // Regular fields from orders
          'test_view_nested_join_path.orders_status',
          'test_view_nested_join_path.orders_count',
        ])
      );
    });

    it('folders with nested join_path syntax (a.b and a.b.c)', async () => {
      const view = metaTransformer.cubes.find(
        (it) => it.config.name === 'test_view_nested_join_path'
      );

      expect(view?.config.folders.length).toBe(4);

      const ordersFolder = view?.config.folders.find(
        (it) => it.name === 'Orders Folder'
      );
      expect(ordersFolder?.members).toEqual(
        expect.arrayContaining([
          'test_view_nested_join_path.orders_count',
          'test_view_nested_join_path.orders_id',
          'test_view_nested_join_path.orders_number',
          'test_view_nested_join_path.orders_status',
        ])
      );

      const usersFolder = view?.config.folders.find(
        (it) => it.name === 'Users via Orders'
      );
      expect(usersFolder?.members).toEqual(
        expect.arrayContaining([
          'test_view_nested_join_path.users_age',
          'test_view_nested_join_path.users_state',
          'test_view_nested_join_path.users_city',
          'test_view_nested_join_path.users_gender',
        ])
      );

      const addressesFolder = view?.config.folders.find(
        (it) => it.name === 'Addresses via Users'
      );
      expect(addressesFolder?.members).toEqual(
        expect.arrayContaining([
          'test_view_nested_join_path.addresses_street',
          'test_view_nested_join_path.addresses_zip_code',
        ])
      );

      const mixedNestedFolder = view?.config.folders.find(
        (it) => it.name === 'Mixed Nested Folder'
      );
      expect(mixedNestedFolder?.members).toEqual(
        expect.arrayContaining([
          // From orders.users join_path
          'test_view_nested_join_path.users_age',
          'test_view_nested_join_path.users_state',
          'test_view_nested_join_path.users_city',
          'test_view_nested_join_path.users_gender',
          // From orders.users.addresses join_path
          'test_view_nested_join_path.addresses_street',
          'test_view_nested_join_path.addresses_zip_code',
          // Regular fields from orders
          'test_view_nested_join_path.orders_status',
          'test_view_nested_join_path.orders_count',
        ])
      );
    });
  });
}
