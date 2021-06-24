import { exec } from 'child_process';
import HttpsProxyAgent from 'http-proxy-agent';

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

export async function getProxySettings() {
  const [proxy] = (
    await Promise.all([getCommandOutput('npm config get https-proxy'), getCommandOutput('npm config get proxy')])
  )
    .map((s) => s.trim())
    .filter((s) => !['null', 'undefined', ''].includes(s));

  return proxy;
}

export async function getHttpAgentForProxySettings() {
  const proxy = await getProxySettings();

  return proxy ? HttpsProxyAgent(proxy) : undefined;
}
