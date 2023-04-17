import { Readable } from 'stream';

export type Environment = {
  cube: {
      port: number;
      logs: Readable;
  };
  data?: {
      port: number;
      logs: Readable;
  };
  stop: () => Promise<void>;
};
