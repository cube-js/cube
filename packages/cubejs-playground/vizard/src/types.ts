import { PivotConfig, Query } from '@cubejs-client/core';

import { ALL_VIZARD_OPTIONS } from './options';

export type VisualType = (typeof ALL_VIZARD_OPTIONS)['visualization'][number];
export type FrameworkType = (typeof ALL_VIZARD_OPTIONS)['framework'][number];
export type LanguageType = (typeof ALL_VIZARD_OPTIONS)['language'][number];
export type LibraryType = (typeof ALL_VIZARD_OPTIONS)['library'][number];

export type VisualParams = {
  visualization: (typeof ALL_VIZARD_OPTIONS)['visualization'][number];
  framework: (typeof ALL_VIZARD_OPTIONS)['framework'][number];
  language: (typeof ALL_VIZARD_OPTIONS)['language'][number];
  library: (typeof ALL_VIZARD_OPTIONS)['library'][number];
};

export type ConnectionParams = {
  useWebSockets: boolean;
  useSubscription: boolean;
};

export type AllParams = VisualParams & ConnectionParams;

export type ApiParams = {
  apiUrl: string | null;
  apiToken: string | null;
  query: Query;
  pivotConfig: PivotConfig;
};

export type ChartType = 'area' | 'bar' | 'doughnut' | 'line' | 'pie' | 'table';

export type Config = {
  apiUrl: string | null;
  apiToken: string | null;
  useWebSockets: boolean;
  useSubscription: boolean;
  query: Query;
  pivotConfig: PivotConfig;
  chartType: ChartType;
};

export interface FileSystemTree {
  [name: string]: DirectoryNode | FileNode;
}
/**
 * Represents a directory, see {@link FileSystemTree}.
 */
export interface DirectoryNode {
  directory: FileSystemTree;
}
/**
 * Represents a file, see {@link FileSystemTree}.
 */
export interface FileNode {
  file: {
    /**
     * The contents of the file, either as a UTF-8 string or as raw binary.
     */
    contents: string | Uint8Array;
  };
}
