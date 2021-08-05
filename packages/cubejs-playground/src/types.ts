export type UIFramework = 'react' | 'vue' | 'angular';

export type PlaygroundEvent = 'chart' | 'credentials' | 'refetch';

export type QueryMemberKey = 'measures' | 'dimensions' | 'timeDimensions';

export type Credentials = {
  apiUrl: string;
  token: string;
};
