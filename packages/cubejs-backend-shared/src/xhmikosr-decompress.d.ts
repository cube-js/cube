/**
 * `@xhmikosr/decompress` ships no type declarations of its own, so we declare the small
 * surface we rely on here. See https://github.com/XhmikosR/decompress
 */
declare module '@xhmikosr/decompress' {
  interface DecompressFile {
    data: Buffer;
    mode: number;
    mtime: string;
    path: string;
    type: 'directory' | 'file' | 'link' | 'symlink';
  }

  interface DecompressOptions {
    filter?: (file: DecompressFile) => boolean;
    map?: (file: DecompressFile) => DecompressFile;
    plugins?: unknown[];
    strip?: number;
  }

  export default function decompress(
    input: string | Buffer,
    output?: string,
    options?: DecompressOptions
  ): Promise<DecompressFile[]>;
}
