import { useEffect, useState } from 'react';
import { fetch } from 'whatwg-fetch';


export default function useLivePreview() {
  const [status, setStatus] = useState({
    loading: true,
    enabled: false
  });

  useEffect(() => {
    fetch('/playground/live-preview/status')
      .then(res => res.json())
      .then((status) => setStatus({
        loading: false,
        ...status
      }));
  }, []);

  return {
    statusLivePreview: status,
    stopLivePreview: async (): Promise<Boolean> => {
      await fetch('/playground/live-preview/stop');
      return true;
    },
    startLivePreview: (): Promise<Boolean> => {
      return new Promise((resolve, reject) => {
        // const wn = window.open('https://cubecloud.dev/auth/live-preview', '', `width=640,height=720`);
        const wn = window.open('http://localhost:4200/auth/live-preview', '', `width=640,height=720`);

        if (!wn) {
          console.error('The popup was blocked by the browser');

          reject();

          return;
        }

        const interval = setInterval(() => {
          if (wn.closed) {
            clearInterval(interval);
            resolve(true);
          }
        }, 1000);
      });
    }
  };
}
