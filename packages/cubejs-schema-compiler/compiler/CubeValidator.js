const ajv = require("ajv")();
const validateSchema = ajv.compile(require("./schema/cube"));

class CubeValidator {
  constructor(cubeSymbols) {
    this.cubeSymbols = cubeSymbols;
    this.validCubes = {};
  }

  compile(cubes, errorReporter) {
    return this.cubeSymbols.cubeList.map(v =>
      this.validate(
        this.cubeSymbols.getCubeDefinition(v.name),
        errorReporter.inContext(`${v.name} cube`)
      )
    );
  }

  validate(cube, errorReporter) {
    const valid = validateSchema(cube);
    if (!valid) {
      errorReporter.error(ajv.errorsText(validateSchema.errors));
    } else {
      this.validCubes[cube.name] = true;
    }
  }

  isCubeValid(cube) {
    return this.validCubes[cube.name];
  }
}

module.exports = CubeValidator;
