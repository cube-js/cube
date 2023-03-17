import * as React from 'react';
import { useState, useEffect, useMemo, type FC } from 'react';
import { langs } from './dictionary';

import classnames from 'classnames/bind';
import * as classes from './CodeTabs.module.css';
const cn = classnames.bind(classes);

interface CustomEventMap {
  'codetabs.changed': CustomEvent<{ lang: string }>;
}

declare global {
  interface Window {
    addEventListener<K extends keyof CustomEventMap>(
      type: K,
      listener: (this: Document, ev: CustomEventMap[K]) => void
    ): void;
    removeEventListener<K extends keyof CustomEventMap>(
      type: K,
      listener: (this: Window, ev: CustomEventMap[K]) => any,
      options?: boolean | EventListenerOptions
    ): void;
    dispatchEvent<K extends keyof CustomEventMap>(ev: CustomEventMap[K]): void;
  }
}

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
  const tabs = useMemo(
    () =>
      children.reduce<Record<string, number>>((dict, tab, i) => {
        const result = {
          ...dict,
        };
        if (result[tab.props['data-language']] === undefined) {
          result[tab.props['data-language']] = i;
        }
        return result;
      }, {}),
    children
  );

  useEffect(() => {
    const defaultLang = localStorage.getItem(STORAGE_KEY);

    if (defaultLang) {
      if (tabs[defaultLang] !== undefined) {
        setSelectedTab(tabs[defaultLang]);
      }
    }

    const syncHanlder = (e: CustomEvent<{ lang: string }>) => {
      const lang = e.detail.lang;
      if (tabs[lang] !== undefined) {
        setSelectedTab(tabs[lang]);
      }
    };

    const storageHandler = (e: StorageEvent) => {
      if (e.key === STORAGE_KEY) {
        const lang = e.newValue;
        if (lang && tabs[lang] !== undefined) {
          setSelectedTab(tabs[lang]);
        }
      }
    };

    window.addEventListener('storage', storageHandler);
    window.addEventListener('codetabs.changed', syncHanlder);

    return () => {
      window.removeEventListener('storage', storageHandler);
      window.removeEventListener('codetabs.changed', syncHanlder);
    };
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
                key={i}
                className={cn('CodeBlocks__tab', {
                  [classes.SelectedTab]: i === selectedTab,
                })}
                onClick={() => {
                  if (
                    i !== selectedTab &&
                    (lang === 'javascript' || lang === 'yaml')
                  ) {
                    localStorage.setItem(STORAGE_KEY, lang);
                    window.dispatchEvent(
                      new CustomEvent('codetabs.changed', {
                        detail: {
                          lang,
                        },
                      })
                    );
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
