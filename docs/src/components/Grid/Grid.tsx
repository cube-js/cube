import React from 'react';

export type GridProps = {
  children: React.ReactNode;
  cols?: number;
  imageSize?: [height: number, width: number];
  slim?: boolean;
};

export const GridContext = React.createContext('grid');

const defaultProps = {
  cols: 3,
  imageSize: [],
  slim: false,
};

export const COL_CLASS_MAP: Record<number, string> = {
  2: 'gettingStarted',
  3: 'connectingToDatabase',
};

export const Grid = ({
  children,
  ...restProps
}: GridProps) => {

  const normalizedProps = { ...defaultProps, ...restProps };
  const settingsString = JSON.stringify(normalizedProps);
  const wrapperClassName = `${COL_CLASS_MAP[normalizedProps.cols]}Grid`;

  return (
    <GridContext.Provider value={settingsString}>
      <div className={wrapperClassName}>
        <div className="ant-row">
          {children}
        </div>
      </div>
    </GridContext.Provider>
  );
};
