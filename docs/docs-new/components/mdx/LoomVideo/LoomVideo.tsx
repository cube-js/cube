import classnames from 'classnames/bind';

import * as styles from './LoomVideo.module.scss';

const cn = classnames.bind(styles);

export type LoomVideoProps = {
  url: string;
};

export const LoomVideo = ({ url }: LoomVideoProps) => {
  return (
    <div
      className={cn('LoomVideo__Wrapper')}
      style={{ position: 'relative', paddingBottom: '56.25%', height: 0 }}
    >
      <iframe
        src={url}
        frameBorder="0"
        allowFullScreen={true}
        style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: '100%' }}
      ></iframe>
    </div>
  );
}
