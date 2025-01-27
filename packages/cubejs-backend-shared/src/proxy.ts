import { exec } from 'child_process';
import { ProxyAgent } from 'proxy-agent';

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

// deprecated, use ProxyAgent instead
export async function getProxySettings() {
  const [proxy] = (
    await Promise.all([getCommandOutput('npm config -g get https-proxy'), getCommandOutput('npm config -g get proxy')])
  )
    .map((s) => s.trim())
    .filter((s) => !['null', 'undefined', ''].includes(s));

  return proxy;
}

export async function getHttpAgentForProxySettings() {
  return new ProxyAgent();
}
