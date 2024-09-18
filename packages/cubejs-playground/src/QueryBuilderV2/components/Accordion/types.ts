import { PropsWithChildren, ReactElement, ReactNode } from 'react';
import { Styles } from '@cube-dev/ui-kit';

export type ShowExtra = 'onHover' | boolean;

export type AccordionProps = {
  children: ReactElement<AccordionItemProps> | ReactElement<AccordionItemProps>[];
  qa?: string;
  isLazy?: boolean;
  size?: 'small' | 'normal';
  isSeparated?: boolean;
  titleStyles?: Styles;
  contentStyles?: Styles;
};
export type AccordionContextType = Pick<
  AccordionProps,
  'size' | 'isSeparated' | 'isLazy' | 'titleStyles' | 'contentStyles' | 'qa'
>;
export type AccordionProviderProps = PropsWithChildren<
  Pick<AccordionProps, 'size' | 'isSeparated' | 'isLazy' | 'titleStyles' | 'contentStyles' | 'qa'>
>;
export type AccordionItemProps = {
  title: string | number;
  qa?: string;
  subtitle?: ReactNode;
  children: ReactNode | (() => ReactNode);
  isExpanded?: boolean;
  isDefaultExpanded?: boolean;
  extra?: ReactNode;
  showExtra?: ShowExtra;
  titleStyles?: Styles;
  contentStyles?: Styles;
  isSeparated?: AccordionProps['isSeparated'];
  onToggle?: (isExpanded: boolean) => void;
  onExpand?: () => void;
  onCollapse?: () => void;
};

export interface AccordionNestedContextData {
  items: Set<{
    title: AccordionItemProps['title'];
    key?: string;
    collapse: () => void;
    expand: () => void;
  }>;
}
