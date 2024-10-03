import Redis from 'ioredis'

export class RedisQueryCacheClient {

  private redis: Redis
  private namespace: string
  private logger: any

  public constructor(params: { url: string, namespace: string, logger: any }) {
    this.logger = params.logger
    this.namespace = params.namespace
    this.redis = new Redis(params.url)

    this.redis.on('error', error => {
      this.logger('Redis connection error', { requestId: '', url: params.url, error })
    })

    this.redis.on('ready', () => {
      this.logger('Connected to Redis successfully', { requestId: '', url: params.url, namespace: params.namespace })
    })

    this.redis.on('reconnecting', () => {
      this.logger('Reconnecting to Redis...', { requestId: '', url: params.url })
    })

    this.redis.on('end', () => {
      this.logger('Disconnected from Redis', { requestId: '', url: params.url })
    })
  }

  public async set(key: string, value: string, expireSeconds?: number): Promise<void> {
    const namespacedKey = this.getNamespacedKey(key)
    if (expireSeconds) {
      await this.redis.set(namespacedKey, value, 'EX', expireSeconds)
    }
    else {
      await this.redis.set(namespacedKey, value)
    }
  }

  public async setJson(key: string, value: Record<string, any>, expireSeconds?: number): Promise<void> {
    const serializedValue = JSON.stringify(value)
    await this.set(key, serializedValue, expireSeconds)
  }

  public async get(key: string): Promise<string | null> {
    const namespacedKey = this.getNamespacedKey(key)
    return await this.redis.get(namespacedKey)
  }

  public async getJson<T = Record<string, any>>(key: string): Promise<T | null> {
    const result = await this.get(key)
    return result ? JSON.parse(result) : null
  }

  public async increment(key: string): Promise<number> {
    const namespacedKey = this.getNamespacedKey(key)
    return await this.redis.incr(namespacedKey)
  }

  public async delete(key: string): Promise<number> {
    const namespacedKey = this.getNamespacedKey(key)
    return await this.redis.del(namespacedKey)
  }

  public async exists(key: string): Promise<boolean> {
    const namespacedKey = this.getNamespacedKey(key)
    return (await this.redis.exists(namespacedKey)) === 1
  }

  public disconnect(): void {
    this.redis.disconnect()
  }

  private getNamespacedKey(key: string): string {
    return `${this.namespace}:${key}`
  }

}
