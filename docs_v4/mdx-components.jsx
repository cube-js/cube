import { useMDXComponents as getThemeComponents } from 'nextra-theme-docs'

const themeComponents = getThemeComponents()

// Stub component that renders children
const Stub = ({ children }) => <>{children}</>

// Stub component that renders nothing
const EmptyStub = () => null

// Stub components - to be replaced with real implementations
const customComponents = {
  // UI Components
  Btn: Stub,
  Screenshot: EmptyStub,
  Diagram: EmptyStub,

  // Code display
  CodeTabs: Stub,

  // Alert boxes
  InfoBox: Stub,
  WarningBox: Stub,
  SuccessBox: Stub,
  ReferenceBox: Stub,

  // Layout
  Grid: Stub,
  GridItem: Stub,

  // Video embeds
  YouTubeVideo: EmptyStub,
  LoomVideo: EmptyStub,

  // Cube-specific components
  CommunitySupportedDriver: Stub,
  QueryBuilder: EmptyStub,
  QueryRenderer: EmptyStub,
  CubeProvider: Stub,
}

export function useMDXComponents(components) {
  return {
    ...themeComponents,
    ...customComponents,
    ...components
  }
}
