import { exec } from 'child_process';
import { ProxyAgent } from 'proxy-agent';
import { HttpsProxyAgent } from 'https-proxy-agent';

let npmProxy: string;
let npmProxyInitialized = false;

function getCommandOutput(command: string) {
  return new Promise<string>((resolve, reject) => {
    exec(command, (error, stdout) => {
      if (error) {
        reject(error.message);
        return;
      }

      resolve(stdout);
    });
  });
}

/**
 * @deprecated
 * use ProxyAgent instead
 */
export async function getProxySettings(): Promise<string> {
  const [proxy] = (
    await Promise.all([getCommandOutput('npm config -g get https-proxy'), getCommandOutput('npm config -g get proxy')])
  )
    .map((s) => s.trim())
    .filter((s) => !['null', 'undefined', ''].includes(s));

  npmProxyInitialized = true;

  return proxy;
}

export async function getHttpAgentForProxySettings() {
  if (!npmProxyInitialized) {
    npmProxy = await getProxySettings();
  }

  if (npmProxy) {
    console.warn('Npm proxy settings are deprecated. Please use HTTP_PROXY, HTTPS_PROXY environment variables instead.');
    return new HttpsProxyAgent(npmProxy);
  }

  return new ProxyAgent();
}
