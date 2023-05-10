import React from 'react';

export type LoomVideoProps = {
  url: string;
};

export const LoomVideo = ({ url }: LoomVideoProps) => {
  return (
    <div
      className="block-video"
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
