interface VersionPart {
  num: number;
  pre: string;
}

function parseVersionParts(v: string): VersionPart[] {
  return v.split('.').map((segment) => {
    const idx = segment.indexOf('-');
    if (idx === -1) {
      return { num: parseInt(segment, 10) || 0, pre: '' };
    }

    return {
      num: parseInt(segment.substring(0, idx), 10) || 0,
      pre: segment.substring(idx + 1),
    };
  });
}

export function isVersionGte(version: string | null, minVersion: string): boolean {
  if (!version) {
    return false;
  }

  const parts = parseVersionParts(version);
  const minParts = parseVersionParts(minVersion);

  for (let i = 0; i < Math.max(parts.length, minParts.length); i++) {
    const a = parts[i] || { num: 0, pre: '' };
    const b = minParts[i] || { num: 0, pre: '' };

    if (a.num > b.num) {
      return true;
    }

    if (a.num < b.num) {
      return false;
    }

    // The same numeric part — pre-release is less than no pre-release
    if (a.pre && !b.pre) {
      return false;
    }

    if (!a.pre && b.pre) {
      return true;
    }
  }

  return true;
}
