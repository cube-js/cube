import * as React from 'react';
import { useState, type FC } from 'react';
import { langs } from './dictionary';

import classnames from 'classnames/bind';
import * as classes from './CodeTabs.module.css';
const cn = classnames.bind(classes);

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

  return (
    <div className={classes.CodeBlock}>
      <div className={classes.CodeBlocks__tabs}>
        {children.map((tab, i) => {
          const lang = tab.props['data-language'];
          return (
            <div
              className={cn('CodeBlocks__tab', {
                [classes.SelectedTab]: i === selectedTab,
              })}
              onClick={() => setSelectedTab(i)}
            >
              {langs[lang] || lang}
            </div>
          );
        })}
      </div>

      {children[selectedTab].props.children}
    </div>
  );
};
