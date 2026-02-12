export interface RedshiftCredentialsProvider {
  getCredentials(): Promise<{ user: string; password: string }>;
}

export class RedshiftPlainCredentialsProvider implements RedshiftCredentialsProvider {
  public constructor(
    private readonly user: string,
    private readonly password: string,
  ) {}

  public async getCredentials() {
    return { user: this.user, password: this.password };
  }
}
