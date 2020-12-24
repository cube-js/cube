import React, { useContext, useEffect, useState } from 'react';
import lockr from 'lockr';

export const FRAMEWORKS = [
  {
    name: 'Vanilla JS',
    slug: 'vanilla',
  },
  {
    name: 'React',
    slug: 'react',
  },
  {
    name: 'Vue',
    slug: 'vue',
  },
  {
    name: 'Angular',
    slug: 'angular',
  },
];

const FrameworkContext = React.createContext<
  [string, (newVal: string) => void]
>([FRAMEWORKS[0].slug, () => {}]);

const FrameworkOfChoiceStore: React.FC = (props) => {
  if (props === undefined) {
    throw new Error(
      'Props are undefined. You probably mixed up between default/named import'
    );
  }

  let defaultValue = FRAMEWORKS[0].slug;

  if (!FRAMEWORKS.map((variant) => variant.slug).includes(defaultValue)) {
    defaultValue = FRAMEWORKS[0].slug;
  }

  const [framework, setFramework] = useState<string>(defaultValue);

  useEffect(() => {
    setFramework(lockr.get('frameworkOfChoice') || defaultValue);
  }, []);

  useEffect(() => {
    lockr.set('frameworkOfChoice', framework);
  }, [framework]);

  return (
    <FrameworkContext.Provider value={[framework, setFramework]}>
      {props.children}
    </FrameworkContext.Provider>
  );
};

export const useFrameworkOfChoice = () => useContext(FrameworkContext);

export default FrameworkOfChoiceStore;
