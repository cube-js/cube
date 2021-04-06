import { createContext, useState, useEffect } from 'react';

type TLivePreviewContextProps = {
  livePreviewDisabled: Boolean;
  statusLivePreview: any;
  createTokenWithPayload: (payload) => Promise<any>; 
  stopLivePreview: () => Promise<Boolean>;
  startLivePreview: () => Promise<Boolean>;
};

export const LivePreviewContextContext = createContext<TLivePreviewContextProps>(
  {} as TLivePreviewContextProps
);

const useLivePreview = (disabled = false) => {
  const [status, setStatus] = useState({
    loading: true,
    enabled: false,
    deploymentUrl: null
  });

  useEffect(() => {
    if (disabled) return;
    const statusPoolingInterval = setInterval(() => { fetchStatus(); }, 5000);
    fetchStatus();
    return () => {
      clearInterval(statusPoolingInterval);
    }
  }, [])

  const fetchStatus = () => {
    return fetch('/playground/live-preview/status')
      .then(res => res.json())
      .then((status) => setStatus({
        loading: false,
        ...status
      }));
  }

  return {
    statusLivePreview: status,
    createTokenWithPayload: async (payload): Promise<Boolean> => {
      const res = await fetch('/playground/live-preview/token', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload)
      });
      return res.json();
    },
    stopLivePreview: async (): Promise<Boolean> => {
      await fetch('/playground/live-preview/stop');
      fetchStatus();
      return true;
    },
    startLivePreview: (): Promise<Boolean> => {
      return new Promise((resolve, reject) => {
        const wn = window.open('https://cubecloud.dev/auth/live-preview', '', `width=640,height=720`);

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
};

export default function LivePreviewContextProvider({ disabled = false, children }) {
  const devModeHooks = useLivePreview(disabled);

  return (
    <LivePreviewContextContext.Provider value={{...devModeHooks, livePreviewDisabled: disabled}}>
      {children}
    </LivePreviewContextContext.Provider>
  );
}
