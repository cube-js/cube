import HttpsProxyAgent from 'http-proxy-agent';
// @ts-ignore
import npmConf from 'npm-conf';

export async function getProxySettings() {
  const conf = npmConf();
  return conf.get('https-proxy') ?? conf.get('proxy');
}

export async function getHttpAgentForProxySettings() {
  const proxy = await getProxySettings();

  return proxy ? HttpsProxyAgent(proxy) : undefined;
}
