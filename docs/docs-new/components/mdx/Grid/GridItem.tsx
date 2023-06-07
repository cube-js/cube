import React from 'react';

import { GridContext } from './Grid';

export type GridItemProps = {
  imageUrl: string;
  title: string;
  url: string;
};

export const GridItem = ({
  imageUrl,
  title,
  url,
}: GridItemProps) => {
  return (
    <GridContext.Consumer>
      {(value) => {
        const settings = JSON.parse(value);
        const [ width, height ] = settings.imageSize;

        return (
          <div className="grid-item">
            <a href={url}>
              <div>
                <div className="grid-item-image">
                  <img
                    src={imageUrl}
                    alt={title}
                    width={width}
                    height={height}
                  />
                </div>
                <div className="grid-item-title">{title}</div>
              </div>
            </a>
          </div>
        );
      }}
    </GridContext.Consumer>
  );
};
