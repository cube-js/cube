import React from 'react';

import { COL_CLASS_MAP, GridContext } from './Grid';

export type GridItemProps = {
  imageUrl: string;
  title: string;
  url: string;
};

const WRAPPER_CLASS_MAP: Record<number, string> = {
  2: 'ant-col ant-col-xs-24 ant-col-sm-24 ant-col-md-24 ant-col-xl-12',
  3: 'ant-col ant-col-xs-24 ant-col-sm-24 ant-col-md-24 ant-col-lg-12 ant-col-xl-8',
}

export const GridItem = ({ imageUrl, title, url  }: GridItemProps) => {
  return (
    <GridContext.Consumer>
      {(value) => {
        const settings = JSON.parse(value);
        const classPrefix = COL_CLASS_MAP[settings.cols];
        const [ height, width ] = settings.imageSize;

        const wrapperClassName = [
          `${classPrefix}Item`,
          WRAPPER_CLASS_MAP[settings.cols],
          settings.slim ? `${classPrefix}ItemSlim` : ''
        ].join(' ');

        return (
          <div className={wrapperClassName}>
            <a href={url}>
              <div className={`${classPrefix}ItemContent`}>
                <div className={`${classPrefix}ItemImage`}>
                  <img
                    src={imageUrl}
                    alt={title}
                    width={width}
                    height={height}
                  />
                </div>
                <div className={`${classPrefix}ItemLink`}>{title}</div>
              </div>
            </a>
          </div>
        )
      }}
    </GridContext.Consumer>
  );
};
