import { useMDXComponents as getThemeComponents } from 'nextra-theme-docs'
import { Btn } from './components/mdx/Btn'
import { InfoBox, WarningBox, SuccessBox, ReferenceBox } from './components/mdx/AlertBox'

const themeComponents = getThemeComponents()

// Stub component that renders children
const Stub = ({ children }) => <>{children}</>

// Stub component that renders nothing
const EmptyStub = () => null

// Stub components - to be replaced with real implementations
const customComponents = {
  // UI Components
  Btn,
  Screenshot: EmptyStub,
  Diagram: EmptyStub,

  // Code display
  CodeTabs: Stub,

  // Alert boxes
  InfoBox,
  WarningBox,
  SuccessBox,
  ReferenceBox,

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
