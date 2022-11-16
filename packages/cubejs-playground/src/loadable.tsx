import React, { lazy, createElement } from 'react';

type LoadOptions = {
  delay?: number;
};

type LoadFunction = (
  action?: (...args: any) => unknown | Promise<unknown>,
  toggle?: Function,
  options?: LoadOptions
) => Promise<void>;

type LoadableComponent = {
  loaded: boolean;
  load: LoadFunction;
};

type C = React.LazyExoticComponent<React.ComponentType<any>> & LoadableComponent;

function handleChunkLoadError(error: Error) {
  if (!error.message.includes('Failed to fetch dynamically')) {
    throw error;
  }

  const lastReloadTime = Number(localStorage.getItem('lastLocationReload') || '0');

  if (Date.now() - Number(lastReloadTime) >= 60_000) {
    window.location.reload();
    localStorage.setItem('lastLocationReload', Date.now().toString());
  }
}

export function loadable(
  factory: () => Promise<{
    default: React.ComponentType<any>;
  }>,
  timeout?: number
) {
  const Component = loadable(async () => {
    try {
      return await factory();
    } catch (error: any) {
      handleChunkLoadError(error);
    }

    return {
      default: () => createElement('div', null, null),
    };
  }) as C;

  Component.loaded = false;
  Component.load = async (toggle, action) => {
    if (Component.loaded) {
      action?.();
      return;
    }

    toggle?.();
    try {
      await factory();
    } catch (error: any) {
      handleChunkLoadError(error);
    }
    toggle?.();
    Component.loaded = true;
    action?.();
  };

  if (timeout) {
    setTimeout(() => {
      if (!Component.loaded) {
        void Component.load();
      }
    }, timeout);
  }

  return Component;
}
