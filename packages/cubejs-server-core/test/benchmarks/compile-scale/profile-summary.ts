/**
 * Ranks functions by self time across the .cpuprofile files of one run (the main thread and any
 * worker threads).
 *
 *   node dist/test/benchmarks/compile-scale/profile-summary.js <dir-or-file> [top]
 */
import fs from 'fs';
import path from 'path';

type ProfileNode = {
  id: number;
  callFrame: { functionName: string; url: string; lineNumber: number };
  children?: number[];
};

type Profile = {
  nodes: ProfileNode[];
  samples: number[];
  timeDeltas: number[];
  startTime: number;
  endTime: number;
};

const shortUrl = (url: string) => {
  const m = url.match(/(?:packages|node_modules)\/.*$/);
  return m ? m[0] : url.replace(/^file:\/\//, '');
};

function selfTimes(profile: Profile): Map<string, number> {
  const byId = new Map(profile.nodes.map((n) => [n.id, n]));
  const out = new Map<string, number>();
  profile.samples.forEach((id, i) => {
    const { callFrame } = byId.get(id)!;
    const key = `${callFrame.functionName || '(anonymous)'}  ${shortUrl(callFrame.url)}${callFrame.url ? `:${callFrame.lineNumber + 1}` : ''}`;
    // timeDeltas[i] is the gap before sample i; attribute it to that sample
    out.set(key, (out.get(key) || 0) + (profile.timeDeltas[i] || 0) / 1000);
  });
  return out;
}

export function summarizeProfiles(target: string, top = 30): string {
  const files = fs.statSync(target).isDirectory()
    ? fs.readdirSync(target).filter((f) => f.endsWith('.cpuprofile')).map((f) => path.join(target, f))
    : [target];

  const lines: string[] = [];

  for (const file of files) {
    const profile: Profile = JSON.parse(fs.readFileSync(file, 'utf8'));
    const totalMs = (profile.endTime - profile.startTime) / 1000;
    const self = [...selfTimes(profile).entries()].sort((a, b) => b[1] - a[1]);
    const idle = self.filter(([k]) => k.startsWith('(idle)')).reduce((n, [, v]) => n + v, 0);
    // Worker threads spend most of a run idle; list only the busy ones
    if (totalMs - idle >= 200) {
      lines.push(`\n${path.basename(file)}: ${(totalMs / 1000).toFixed(1)}s wall, ${((totalMs - idle) / 1000).toFixed(1)}s busy`);

      for (const [k, v] of self.filter(([key]) => !key.startsWith('(idle)')).slice(0, top)) {
        lines.push(`  ${(v / 1000).toFixed(2).padStart(7)}s ${((v / totalMs) * 100).toFixed(1).padStart(5)}%  ${k}`);
      }
    }
  }

  return lines.join('\n');
}

if (require.main === module) {
  const [target, top] = process.argv.slice(2);
  if (!target) {
    console.error('Usage: profile-summary <dir-or-file> [top]');
    process.exit(1);
  }
  console.log(summarizeProfiles(target, top ? Number(top) : 30));
}
