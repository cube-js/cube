import { createContext, useState, useEffect, useRef, ReactNode } from 'react';

import { openWindow } from '../../shared/helpers';
import { Credentials } from '../../types';

export type LivePreviewStatus = {
  lastHashTarget: string;
  uploading: boolean;
  active: boolean;
  deploymentUrl?: string | null;
  lastHash?: string;
  loading?: boolean;
  status?: 'loading' | 'inProgress' | 'running';
  deploymentId?: number;
  url?: string;
};

export type LivePreviewContextProps = {
  credentials: Credentials | null;
  livePreviewDisabled: Boolean;
  statusLivePreview: LivePreviewStatus;
  createTokenWithPayload: (payload) => Promise<any>;
  stopLivePreview: () => Promise<Boolean>;
  startLivePreview: () => Promise<Boolean>;
};

export const LivePreviewContextContext =
  createContext<LivePreviewContextProps | null>(null);

const useLivePreview = (disabled = false) => {
  const activeRef = useRef<boolean>(false);
  const [credentials, setCredentials] = useState<Credentials | null>(null);
  const [status, setStatus] = useState<any>({
    loading: true,
    active: false,
    deploymentUrl: null,
  });

  useEffect(() => {
    if (disabled) {
      return;
    }

    const statusPoolingInterval = setInterval(() => {
      fetchStatus();
    }, 5000);

    fetchStatus();

    return () => {
      clearInterval(statusPoolingInterval);
    };
  }, []);

  // useEffect(() => {
  //   handleChange();
  // }, []);

  useEffect(() => {
    if (!status.loading && activeRef.current !== status.active) {
      handleChange();
    }
    activeRef.current = status.active;
  }, [activeRef, status.active, status.loading]);

  // useEffect(() => {
  //   if (!status.loading && status.active) {
  //     handleChange();
  //   }
  // }, [status]);

  const fetchStatus = () => {
    return fetch('playground/live-preview/status')
      .then((res) => res.json())
      .then((status) => {
        setStatus({
          loading: false,
          ...status,
        });
      });
  };

  const createTokenWithPayload = async (payload): Promise<any> => {
    const res = await fetch('playground/live-preview/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });
    return res.json();
  };

  const handleChange = async () => {
    if (status?.active) {
      const { token } = await createTokenWithPayload({});
      setCredentials({
        token: token?.token || null,
        apiUrl: status?.deploymentUrl || null,
      });
    } else {
      setCredentials(null);
    }
  };

  return {
    credentials,
    statusLivePreview: status,
    createTokenWithPayload,
    stopLivePreview: async (): Promise<Boolean> => {
      await fetch('playground/live-preview/stop');
      await fetchStatus();
      return true;
    },
    startLivePreview: (): Promise<Boolean> => {
      return new Promise((resolve, reject) => {
        const callbackUrl = encodeURIComponent(window.location.origin);
        const params: any =
          window.location.origin !== 'http://localhost:4000' &&
          new URLSearchParams({ callbackUrl }).toString();

        const wn = openWindow({
          url: `https://cubecloud.dev/auth/live-preview${
            params ? `?${params}` : ''
          }`,
        });

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
    },
  };
};

type LivePreviewContextProviderProps = {
  disabled: boolean;
  children: ReactNode;
};

export function LivePreviewContextProvider({
  disabled = false,
  children,
}: LivePreviewContextProviderProps) {
  const devModeHooks = useLivePreview(disabled);

  return (
    <LivePreviewContextContext.Provider
      value={{
        ...devModeHooks,
        livePreviewDisabled: disabled,
      }}
    >
      {children}
    </LivePreviewContextContext.Provider>
  );
}
