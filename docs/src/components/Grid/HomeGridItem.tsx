import React from 'react';

import { GridContext, GridProps } from './Grid';

export type HomeGridItemProps = {
  description: string;
  image: string;
  title: string;
  url: string;
};

export const HomeGridItem = ({
  description,
  image,
  title,
  url,
}: HomeGridItemProps) => {
  return (
    <GridContext.Consumer>
      {(value) => {
        const settings = JSON.parse(value) as GridProps;
        const [ width, height ] = settings.imageSize ?? [];

        return (
          <div className="grid-item">
            <a href={url}>
              <div className="card card--home">
                <div className="card__image">
                  <img
                    src={image}
                    alt={title}
                    width={width}
                    height={height}
                  />
                </div>
                <div className="card__text card__text--right">
                  <div className="card__title">{title}</div>
                  <div className="card__description">{description}</div>
                </div>
              </div>
            </a>
          </div>
        );
      }}
    </GridContext.Consumer>
  );
};
