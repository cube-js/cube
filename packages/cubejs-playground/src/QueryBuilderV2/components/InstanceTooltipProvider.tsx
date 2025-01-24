import { CubeTooltipProviderProps, tasty, TooltipProvider } from '@cube-dev/ui-kit';
import { RefObject } from 'react';

import { useHasOverflow } from '../hooks';
import { titleize } from '../utils';

const TooltipWrapper = tasty({
  styles: {
    Name: {
      display: 'block',
      width: 'max-content',
      preset: 't4m',
    },

    Title: {
      display: 'block',
      width: 'max-content',
      preset: 't3',
    },

    Description: {
      display: 'block',
      preset: 'p3',
    },

    Type: {
      preset: 'c2',
      opacity: 0.7,
    },
  },
});

interface InstanceTooltipProviderProps {
  name: string;
  fullName?: string;
  title?: string;
  type?: 'dimension' | 'measure' | 'hierarchy' | 'folder' | 'segment';
  description?: string;
  forceShown?: boolean;
  children: CubeTooltipProviderProps['children'];
  isDisabled?: boolean;
  overflowRef?: RefObject<HTMLDivElement>;
}

export function InstanceTooltipProvider(props: InstanceTooltipProviderProps) {
  const {
    name,
    fullName,
    type,
    title,
    description,
    children,
    isDisabled,
    forceShown,
    overflowRef,
  } = props;

  const hasOverflow = useHasOverflow(overflowRef);
  const isAutoTitle = titleize(name) === title;

  if ((!forceShown && (isDisabled || (!hasOverflow && isAutoTitle) || !overflowRef)) || !fullName) {
    return children;
  }

  return (
    <TooltipProvider
      title={
        <>
          <TooltipWrapper>
            {type && <div data-element="Type">{type}</div>}
            <div data-element="Name">{fullName}</div>
            <div data-element="Title">{title}</div>
            <div data-element="Description">{description}</div>
          </TooltipWrapper>
        </>
      }
      width="max-content"
      delay={1000}
      placement="right"
    >
      {children}
    </TooltipProvider>
  );
}
