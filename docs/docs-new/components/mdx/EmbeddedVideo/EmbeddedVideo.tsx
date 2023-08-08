import classnames from 'classnames/bind';

import * as styles from './EmbeddedVideo.module.scss';

const cn = classnames.bind(styles);

export type EmbeddedVideoProps = {
  controls?: boolean;
  src: string;
  type?: string;
};

export const EmbeddedVideo = ({
  controls = true,
  src,
  type = "video/mp4"
}: EmbeddedVideoProps) => {
  return (
    <div
      className={cn('EmbeddedVideo__Wrapper')}
      style={{ position: 'relative', paddingBottom: '56.25%', height: 0 }}
    >
      <video controls={controls}>
        <source
          src={src}
          type={type}
        />
      </video>
    </div>
  );
}
