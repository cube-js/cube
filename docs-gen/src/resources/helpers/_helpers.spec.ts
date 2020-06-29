import * as fs from 'fs-extra';
import * as Handlebars from 'handlebars';
import * as path from 'path';
import { Application } from 'typedoc';

const handlebarsHelpersOptionsStub = {
  fn: () => 'true',
  inverse: () => 'false',
  hash: {},
};

describe(`Helpers`, () => {
  let app;
  let project: any;
  const out = path.join(__dirname, 'tmp');

  beforeAll(() => {
    app = new Application();
    app.bootstrap({
      module: 'CommonJS',
      target: 'ES5',
      readme: 'none',
      theme: 'markdown',
      logger: 'none',
      plugin: path.join(__dirname, '../../../dist/index'),
    });
    project = app.convert(app.expandInputFiles(['./test/stubs/']));
    app.generateDocs(project, out);
  });

  afterAll(() => {
    fs.removeSync(out);
  });

  describe(`utils helpers`, () => {
    test(`should compile headings helper`, () => {
      expect(Handlebars.helpers.heading.call(this, 2)).toEqual('##');
    });
    test(`should compile stripLineBreaks helper`, () => {
      const result = Handlebars.helpers.stripLineBreaks.call('line 1\n line2\n');
      expect(result).toMatchSnapshot('line 1  line2');
    });
    test(`should compile spaces helper`, () => {
      const result = Handlebars.helpers.spaces.call(this, 3);
      expect(result).toEqual('!spaces   ');
    });
  });

  describe(`declarationTitle helper`, () => {
    test(`should compi;e`, () => {
      expect(Handlebars.helpers.declarationTitle.call(project.findReflectionByName('color'))).toMatchSnapshot();
    });
  });

  describe(`ifHasTypeDeclarations helper`, () => {
    test(`should return true if ifHasTypeDeclarations is true and expectation is truthy`, () => {
      const result = Handlebars.helpers.ifHasTypeDeclarations.call(
        project.findReflectionByName('drawText').signatures[0],
        true,
        handlebarsHelpersOptionsStub,
      );
      expect(result).toEqual('true');
    });

    test(`should return true if ifHasTypeDeclarations is false and expectation is truthy`, () => {
      const data = project.findReflectionByName('exportedFunction');
      const result = Handlebars.helpers.ifHasTypeDeclarations.call(
        data.signatures[0],
        true,
        handlebarsHelpersOptionsStub,
      );
      expect(result).toEqual('false');
    });

    test(`should return true if ifHasTypeDeclarations is false and expectation is falsey`, () => {
      const data = project.findReflectionByName('exportedFunction');
      const result = Handlebars.helpers.ifHasTypeDeclarations.call(
        data.signatures[0],
        false,
        handlebarsHelpersOptionsStub,
      );
      expect(result).toEqual('true');
    });
  });

  describe(`ifIsLiteralType helper`, () => {
    test(`should return true if isLiteralType is is true and expectation is truthy`, () => {
      const data = project.findReflectionByName('objectLiteral');
      const result = Handlebars.helpers.ifIsLiteralType.call(data, true, handlebarsHelpersOptionsStub);
      expect(result).toEqual('true');
    });

    test(`should return false if isLiteralType is is true and expectation is falsey`, () => {
      const data = project.findReflectionByName('objectLiteral');
      const result = Handlebars.helpers.ifIsLiteralType.call(data, false, handlebarsHelpersOptionsStub);
      expect(result).toEqual('false');
    });

    test(`should return true if isLiteralType is is false and expectation is falsey`, () => {
      const data = project.findReflectionByName('color');
      const result = Handlebars.helpers.ifIsLiteralType.call(data, false, handlebarsHelpersOptionsStub);
      expect(result).toEqual('true');
    });
  });

  describe(`ifParentIsObjectLiteral helper`, () => {
    test(`should return true if ifParentIsObjectLiteral is is true and expectation is truthy`, () => {
      const data = {
        parent: {
          parent: {
            kind: 2097152,
          },
        },
      };
      const result = Handlebars.helpers.ifParentIsObjectLiteral.call(data, true, handlebarsHelpersOptionsStub);
      expect(result).toEqual('true');
    });

    test(`should return false if ifParentIsObjectLiteral is is false and expectation is truthy`, () => {
      const data = {};
      const result = Handlebars.helpers.ifParentIsObjectLiteral.call(data, true, handlebarsHelpersOptionsStub);
      expect(result).toEqual('false');
    });

    test(`should return true if ifParentIsObjectLiteral is is false and expectation is falsey`, () => {
      const data = {};
      const result = Handlebars.helpers.ifParentIsObjectLiteral.call(data, false, handlebarsHelpersOptionsStub);
      expect(result).toEqual('true');
    });
  });

  describe(`literal helper`, () => {
    test(`should compile object literal`, () => {
      const data = project.findReflectionByName('objectLiteral');
      const result = Handlebars.helpers.literal.call(data);
      expect(result).toMatchSnapshot();
    });

    test(`should compile type literal`, () => {
      const data = project.findReflectionByName('typeLiteral');
      const result = Handlebars.helpers.literal.call(data);
      expect(result).toMatchSnapshot();
    });
  });

  describe(`parameterNameAndType helper`, () => {
    test(`sould compile`, () => {
      const data = project.findReflectionByName('objectLiteral');
      const result = Handlebars.helpers.parameterNameAndType.call(data);
      expect(result).toMatchSnapshot();
    });
  });

  describe(`parameterTable helper`, () => {
    test(`should compile`, () => {
      const data = project.findReflectionByName('functionWithDefaults');
      const result = Handlebars.helpers.parameterTable.call(data.signatures[0].parameters);
      expect(result).toMatchSnapshot();
    });
  });

  describe(`signatureTitle helper`, () => {
    test(`should compile`, () => {
      const data = project.findReflectionByName('functionWithParameters');
      const result = Handlebars.helpers.signatureTitle.call(data.signatures[0]);
      expect(result).toMatchSnapshot();
    });
  });

  describe(`typeAndParent helper`, () => {
    test(`should compile`, () => {
      const data = project.findReflectionByName('BaseClass');
      const result = Handlebars.helpers.typeAndParent.call(data.children[3].implementationOf);
      expect(result).toMatchSnapshot();
    });
  });

  describe(`type helper`, () => {
    test(`should compile intrinsic type`, () => {
      const data = project.findReflectionByName('color');
      const result = Handlebars.helpers.type.call(data.type);
      expect(result).toMatchSnapshot();
    });
  });
});
