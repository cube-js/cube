import classnames from 'classnames/bind';

import * as styles from './YoutubeVideo.module.scss';

const cn = classnames.bind(styles);

export type YouTubeVideoProps = {
  url: string;
};

export const YouTubeVideo = ({ url }: YouTubeVideoProps) => {
  return (
    <div
      className={cn('YoutubeVideo__Wrapper')}
      style={{ position: 'relative', paddingBottom: '56.25%', height: 0 }}
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
