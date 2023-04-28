import { MemberReference } from './descriptors/MemberReference';
import { ValueWithComments } from './descriptors/ValueWithComments';
import { BaseSchemaFormatter, JavaScriptSchemaFormatter, YamlSchemaFormatter } from './formatters';
import {
  CubeDescriptor,
  CubeDescriptorMember,
  DatabaseSchema,
  TableName,
} from './ScaffoldingSchema';

export type SchemaContext = {
  dataSource?: string;
};

export type CubeMembers = {
  measures: CubeDescriptorMember[];
  dimensions: CubeDescriptorMember[];
};

export type SchemaDescriptor =
  | SchemaDescriptor[]
  | string
  | number
  | MemberReference
  | ValueWithComments
  | object;

export enum SchemaFormat {
  JavaScript = 'js',
  Yaml = 'yaml',
}

export type ScaffoldingTemplateOptions = {
  format?: SchemaFormat,
  snakeCase?: boolean
};

export class ScaffoldingTemplate {
  private formatStrategy: BaseSchemaFormatter;

  public constructor(
    dbSchema: DatabaseSchema,
    private readonly driver,
    protected readonly options: ScaffoldingTemplateOptions = {
      snakeCase: false
    }
  ) {
    this.formatStrategy =
      options.format === SchemaFormat.Yaml
        ? new YamlSchemaFormatter(dbSchema, this.driver, { snakeCase: Boolean(this.options.snakeCase) })
        : new JavaScriptSchemaFormatter(dbSchema, this.driver, { snakeCase: Boolean(this.options.snakeCase) });
  }

  public generateFilesByTableNames(
    tableNames: TableName[],
    schemaContext: SchemaContext = {}
  ) {
    return this.formatStrategy.generateFilesByTableNames(
      tableNames,
      schemaContext,
    );
  }

  public generateFilesByCubeDescriptors(
    cubeDescriptors: CubeDescriptor[],
    schemaContext: SchemaContext = {}
  ) {
    return this.formatStrategy.generateFilesByCubeDescriptors(
      cubeDescriptors,
      schemaContext
    );
  }
}
