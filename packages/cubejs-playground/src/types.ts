export type UIFramework = 'react' | 'vue' | 'angular';

export type PlaygroundEvent = 'chart' | 'credentials' | 'refetch';

export type QueryMemberKey = 'measures' | 'dimensions' | 'segments' | 'timeDimensions';

export type Credentials = {
  apiUrl: string;
  token: string;
};

export enum SchemaFormat {
  js = 'js',
  yaml = 'yaml',
}