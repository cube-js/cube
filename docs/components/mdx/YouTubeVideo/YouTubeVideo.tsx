import classnames from 'classnames/bind';

import * as styles from './YoutubeVideo.module.scss';

const cn = classnames.bind(styles);

export type YouTubeVideoProps = {
  url: string;
  aspectRatio?: number;
};

function formatAspectRatioAsPercentage(aspectRatio: number) {
  return `${((1 / aspectRatio) * 100).toFixed(2)}%`;
}

export const YouTubeVideo = ({ url, aspectRatio = 16/9 }: YouTubeVideoProps) => {
  return (
    <div
      className={cn('YoutubeVideo__Wrapper')}
      style={{
        position: 'relative',
        paddingBottom: formatAspectRatioAsPercentage(aspectRatio),
        height: 0
      }}
    >
      <iframe
        src={url}
        frameBorder="0"
        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share; fullscreen;"
        allowFullScreen={true}
        style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: '100%' }}
      ></iframe>
    </div>
  );
}
