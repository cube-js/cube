import { VIZARD_PARAMS_MAP } from './options';

export function useAppName(params: {
  visualization: string;
  library: string;
  language: string;
  framework: string;
}): string {
  const { visualization, library, language, framework } = params;

  if (!VIZARD_PARAMS_MAP[visualization]) {
    throw new Error('Invalid visualization');
  }

  if (!VIZARD_PARAMS_MAP[visualization]?.[framework]) {
    throw new Error('Invalid framework');
  }

  if (!VIZARD_PARAMS_MAP[visualization]?.[framework]?.[language]) {
    throw new Error('Invalid language');
  }

  if (!VIZARD_PARAMS_MAP[visualization]?.[framework]?.[language]?.[library]) {
    throw new Error('Invalid library');
  }

  // @ts-expect-error - we are sure that the keys exist
  return VIZARD_PARAMS_MAP[visualization]?.[framework]?.[language]?.[library];
}
