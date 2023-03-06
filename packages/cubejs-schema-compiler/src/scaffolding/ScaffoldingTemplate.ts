import { MemberReference } from './descriptors/MemberReference';
import { ValueWithComments } from './descriptors/ValueWithComments';
import { JavaScriptSchemaFormatter, YamlSchemaFormatter } from './formatters';
import {
  CubeDescriptor,
  CubeDescriptorMember,
  DatabaseSchema,
  TableName,
} from './ScaffoldingSchema';
import { BaseSchemaFormatter } from './formatters/BaseSchemaFormatter';

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

export class ScaffoldingTemplate {
  private formatStrategy: BaseSchemaFormatter;

  public constructor(
    dbSchema: DatabaseSchema,
    private readonly driver,
    format?: SchemaFormat
  ) {
    this.formatStrategy =
      format === SchemaFormat.Yaml
        ? new YamlSchemaFormatter(dbSchema, this.driver)
        : new JavaScriptSchemaFormatter(dbSchema, this.driver);
  }

  public generateFilesByTableNames(
    tableNames: TableName[],
    schemaContext: SchemaContext = {}
  ) {
    return this.formatStrategy.generateFilesByTableNames(
      tableNames,
      schemaContext
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
