// `index.js` returns the manifest of this release. The converter builds it with these types.

declare namespace manifest {
  /** A JSON Schema (draft-07) node, limited to the keywords the manifest uses. */
  type JsonSchema = boolean | JsonSchemaObject;

  interface JsonSchemaObject {
    $schema?: string;
    $ref?: string;
    type?: string | string[];
    description?: string;
    deprecated?: boolean;
    enum?: unknown[];
    pattern?: string;
    minItems?: number;
    maxItems?: number;
    minLength?: number;
    maxLength?: number;
    properties?: Record<string, JsonSchema>;
    patternProperties?: Record<string, JsonSchema>;
    required?: string[];
    additionalProperties?: boolean;
    items?: JsonSchema;
    anyOf?: JsonSchema[];
    allOf?: JsonSchema[];
    not?: JsonSchema;
    if?: JsonSchema;
    then?: JsonSchema;
    else?: JsonSchema;
    definitions?: Record<string, JsonSchema>;
  }

  /** The root schema of a model file, with the parts it refers to by `$ref` in `definitions`. */
  interface ModelSchemaManifest extends JsonSchemaObject {
    definitions: Record<string, JsonSchema>;
  }
}

declare const manifest: manifest.ModelSchemaManifest;

export = manifest;
