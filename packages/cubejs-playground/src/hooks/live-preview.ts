import { useEffect, useState } from 'react';
import { fetch } from 'whatwg-fetch';


export default function useLivePreview() {
  const [status, setStatus] = useState({
    loading: true,
    enabled: false,
    deploymentUrl: null
  });

  useEffect(() => {
    fetchStatus();
    const statusPoolingInterval = setInterval(()=>{
      fetchStatus();
    }, 5000);
    return () => {
      clearInterval(statusPoolingInterval);
    }
  }, [])

  const fetchStatus = () => {
    fetch('/playground/live-preview/status')
      .then(res => res.json())
      .then((status) => setStatus({
        loading: false,
        ...status
      }));
  }

  useEffect(() => {
    
  }, []);

  return {
    statusLivePreview: status,
    stopLivePreview: async (): Promise<Boolean> => {
      await fetch('/playground/live-preview/stop');
      fetchStatus();
      return true;
    },
    startLivePreview: (): Promise<Boolean> => {
      return new Promise((resolve, reject) => {
        const wn = window.open('https://cubecloud.dev/auth/live-preview', '', `width=640,height=720`);
        // const wn = window.open('http://localhost:4200/auth/live-preview', '', `width=640,height=720`);

        if (!wn) {
          console.error('The popup was blocked by the browser');

          reject();

          return;
        }

        const interval = setInterval(() => {
          if (wn.closed) {
            clearInterval(interval);
            resolve(true);
            fetchStatus();
          }
        }, 1000);
      });
    }
  };
}
