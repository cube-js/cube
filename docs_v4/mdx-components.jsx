import { useMDXComponents as getThemeComponents } from 'nextra-theme-docs'

const themeComponents = getThemeComponents()

export function useMDXComponents(components) {
  return {
    ...themeComponents,
    ...components
  }
}
