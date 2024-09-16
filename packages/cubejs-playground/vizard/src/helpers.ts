import { ALL_VIZARD_OPTIONS, VIZARD_PARAMS_MAP } from './options';
import {
  FrameworkType,
  LanguageType,
  LibraryType,
  VisualParams,
  VisualType,
} from './types';

export function validateVisualParams(
  params: Partial<VisualParams>
): VisualParams {
  const { visualization, framework, language, library } = params;
  let initialVisualization, initialFramework, initialLanguage, initialLibrary;

  if (!visualization || !VIZARD_PARAMS_MAP[visualization]) {
    initialVisualization = Object.keys(VIZARD_PARAMS_MAP)[0];
    initialFramework = Object.keys(
      VIZARD_PARAMS_MAP[initialVisualization as VisualType] || {}
    )[0];
    initialLanguage = Object.keys(
      VIZARD_PARAMS_MAP[initialVisualization as VisualType]?.[
        initialFramework as FrameworkType
      ] || {}
    )[0];
    initialLibrary = Object.keys(
      VIZARD_PARAMS_MAP[initialVisualization as VisualType]?.[
        initialFramework as FrameworkType
      ]?.[initialLanguage as LanguageType] || {}
    )[0];

    return {
      visualization: initialVisualization as VisualType,
      framework: initialFramework as FrameworkType,
      language: initialLanguage as LanguageType,
      library: initialLibrary as LibraryType,
    };
  }

  if (!framework || !VIZARD_PARAMS_MAP[visualization]?.[framework]) {
    initialFramework = Object.keys(
      VIZARD_PARAMS_MAP[visualization as VisualType] || {}
    )[0];
    initialLanguage = Object.keys(
      VIZARD_PARAMS_MAP[visualization as VisualType]?.[
        initialFramework as FrameworkType
      ] || {}
    )[0];
    initialLibrary = Object.keys(
      VIZARD_PARAMS_MAP[visualization as VisualType]?.[
        initialFramework as FrameworkType
      ]?.[initialLanguage as LanguageType] || {}
    )[0];

    return {
      visualization,
      framework: initialFramework as FrameworkType,
      language: initialLanguage as LanguageType,
      library: initialLibrary as LibraryType,
    };
  }

  if (!language || !VIZARD_PARAMS_MAP[visualization]?.[framework]?.[language]) {
    initialLanguage = Object.keys(
      VIZARD_PARAMS_MAP[visualization as VisualType]?.[
        framework as FrameworkType
      ] || {}
    )[0];
    initialLibrary = Object.keys(
      VIZARD_PARAMS_MAP[visualization as VisualType]?.[
        framework as FrameworkType
      ]?.[initialLanguage as LanguageType] || {}
    )[0];

    return {
      visualization,
      framework,
      language: initialLanguage as LanguageType,
      library: initialLibrary as LibraryType,
    };
  }

  if (
    !library ||
    !VIZARD_PARAMS_MAP[visualization]?.[framework]?.[language]?.[library]
  ) {
    initialLibrary = Object.keys(
      VIZARD_PARAMS_MAP[visualization as VisualType]?.[
        framework as FrameworkType
      ]?.[language as LanguageType] || {}
    )[0];

    return {
      visualization,
      framework,
      language,
      library: initialLibrary as LibraryType,
    };
  }

  return params as VisualParams;
}

export function getAvailableOptions(params: VisualParams) {
  const { visualization, framework, language } = params;

  return {
    visualization: ALL_VIZARD_OPTIONS.visualization,
    framework: ALL_VIZARD_OPTIONS.framework.filter((framework) => {
      return VIZARD_PARAMS_MAP[visualization]?.[framework];
    }),
    language: ALL_VIZARD_OPTIONS.language.filter((language) => {
      return VIZARD_PARAMS_MAP[visualization]?.[framework]?.[language];
    }),
    library: ALL_VIZARD_OPTIONS.library.filter((library) => {
      return VIZARD_PARAMS_MAP[visualization]?.[framework]?.[language]?.[
        library
      ];
    }),
  };
}
