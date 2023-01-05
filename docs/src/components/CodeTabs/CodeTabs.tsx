import * as React from 'react';
import { useState, useEffect, type FC } from 'react';
import { langs } from './dictionary';

import classnames from 'classnames/bind';
import * as classes from './CodeTabs.module.css';
const cn = classnames.bind(classes);

const STORAGE_KEY = 'cube-docs.default-code-lang';

export interface CodeTabsProps {
  children: Array<{
    props: {
      'data-language': string;
      children: any;
    };
  }>;
}

export const CodeTabs: FC<CodeTabsProps> = ({ children }) => {
  const [selectedTab, setSelectedTab] = useState(0);

  useEffect(() => {
    const defaultLang = localStorage.getItem(STORAGE_KEY);

    if (defaultLang) {
      children.some((tab, i) => {
        if (tab.props['data-language'] === defaultLang) {
          setSelectedTab(i);
        }
      });
    }
  }, []);

  return (
    <div className={classes.CodeBlock}>
      <div className={classes.CodeBlocks__tabs}>
        {children
          .filter((tab) => !!tab.props['data-language'])
          .map((tab, i) => {
            let lang = tab.props['data-language'];
            if (lang === 'js') {
              lang = 'javascript';
            }
            return (
              <div
                className={cn('CodeBlocks__tab', {
                  [classes.SelectedTab]: i === selectedTab,
                })}
                onClick={() => {
                  if (lang === 'javascript' ||  lang === 'yaml') {
                    localStorage.setItem(STORAGE_KEY, lang);
                  }
                  setSelectedTab(i);
                }}
              >
                {langs[lang] || lang}
              </div>
            );
          })}
      </div>

      {children && children[selectedTab].props.children}
    </div>
  );
};
