import { ProxyAgent } from 'proxy-agent';

export async function getHttpAgentForProxySettings() {
  if (!process.env.HTTP_PROXY && !process.env.HTTPS_PROXY) {
    return undefined;
  }
  return new ProxyAgent();
}
