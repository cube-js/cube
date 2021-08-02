import React, { useEffect, useState } from 'react';

const GitHubCodeBlock = (props: propsType) => {
  const { href, titleSuffixCount, part, lang } = props;
  const [file, setFile] = useState("");
  const [title, setTitle] = useState("");

  useEffect(() => {
    const { user, repo, branch, filePath, title } = parseHref(href, titleSuffixCount);
    
    setTitle(title);
    fetchCodeFromGitHub(
      `https://raw.githubusercontent.com/${user}/${repo}/${branch}/${filePath}`, setFile, part
    );
  }, []);

  return (
    <div>
      <h3>{title}</h3>
      <pre>
        <code className={`language-${lang}`}>
          { file }
        </code>
      </pre>
    </div>
  );
};

export default GitHubCodeBlock;

function parseHref(href: string, titleSuffixCount: number): parseHref {
  const stringWithOutGitHubSlug = href.replace('https://github.com/', '');
  const arrayOfStringData = stringWithOutGitHubSlug.split('/');
  const user = arrayOfStringData?.[0] || null;
  const repo = arrayOfStringData?.[1] || null;
  const branch = arrayOfStringData?.[3] || null;
  const title = [...arrayOfStringData]?.reverse()?.splice(0, titleSuffixCount)?.reverse()?.join('/') || "";
  const filePath = arrayOfStringData?.splice(4)?.join('/') || null;

  return { user, repo, branch, filePath, title };
}

async function fetchCodeFromGitHub(url: string, setFile: (text: string) => void, part: string | null) {
  const response = await fetch(url);
  if (response.ok) {
    let text = await response.text();

    if (!part) {
      setFile(text);
    } else {
      // @todo splice text
      setFile(text);  
    }
  }
  highlightCode();
}

function highlightCode(): void {
  window.Prism && window.Prism.highlightAll();
}

interface propsType {
  href: string;
  titleSuffixCount: number;
  part: string | null;
  lang: string;
}
interface parseHref {
  [key: string]: string | null,
  title: string
}
