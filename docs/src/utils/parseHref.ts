export default function parseHref(href: string, titleSuffixCount: number): parseHref {
  const stringWithOutGitHubSlug = href.replace('https://github.com/', '');
  const arrayOfStringData = stringWithOutGitHubSlug.split('/');
  const user = arrayOfStringData?.[0] || null;
  const repo = arrayOfStringData?.[1] || null;
  const branch = arrayOfStringData?.[3] || null;
  const title =
    [...arrayOfStringData]
      ?.reverse()
      ?.splice(0, titleSuffixCount)
      ?.reverse()
      ?.join('/') || '';
  const filePath = arrayOfStringData?.splice(4)?.join('/') || null;

  return { user, repo, branch, filePath, title };
}

interface parseHref {
  [key: string]: string | null;
  title: string;
}