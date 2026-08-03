import { tasty } from '@cube-dev/ui-kit';
import { useEffect, useRef } from 'react';
import { Config } from './types';

const IFrameElement = tasty({
  as: 'iframe',
  styles: {
    width: '100%',
    height: '100%',
    border: 'none',
  },
});

export interface PreviewProps {
  appName: string;
  config: Config;
}

export function Preview(props: PreviewProps) {
  const { appName, config } = props;
  const ref = useRef<HTMLIFrameElement>(null);

  const hash = encodeURIComponent(btoa(JSON.stringify(config)));
  const src = `/vizard/preview/${appName}/index.html#${hash}`;

  useEffect(() => {
    if (ref?.current) {
      ref.current.contentWindow?.location.reload();
      ref.current.src = src;
    }
  }, [src]);

  return <IFrameElement ref={ref} src={src} />;
}
