import React from 'react';

export type GridProps = {
  children: React.ReactNode;
  cols?: number;
  imageSize?: [ width?: number, height?: number];
};

export const GridContext = React.createContext('grid');

const defaultProps = {
  cols: 3,
  imageSize: [],
};
export const Grid = ({
  children,
  ...restProps
}: GridProps) => {
  const normalizedProps = { ...defaultProps, ...restProps };
  const settingsString = JSON.stringify(normalizedProps);
  const className = `grid__col-${normalizedProps.cols}`;

  return (
    <GridContext.Provider value={settingsString}>
      <div className="ant-row">
        <div className={className}>
          {children}
        </div>
      </div>
    </GridContext.Provider>
  );
};
