export type ContentHash = {
  pathsHash: string;
  contentHash: string;
  paths?: string[];
};

export type DevModeStatus = {
  status: 'running' | 'stopped' | 'spinning';
  lastHash?: string;
  contentHash?: ContentHash;
  deploymentUrl: string;
};
