export interface RedshiftCredentialsProvider {
  getCredentials(): Promise<{ user: string; password: string }>;

  getDbName(): string;
}

export class RedshiftPlainCredentialsProvider implements RedshiftCredentialsProvider {
  public constructor(
    private readonly user: string,
    private readonly password: string,
    private readonly dbName: string
  ) {}

  public getDbName(): string {
    return this.dbName;
  }

  public async getCredentials() {
    return { user: this.user, password: this.password };
  }
}
