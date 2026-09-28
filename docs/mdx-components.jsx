import { useMDXComponents as getThemeComponents } from 'nextra-theme-docs'
import { Btn } from './components/mdx/Btn'
import { InfoBox, WarningBox, SuccessBox, ReferenceBox } from './components/mdx/AlertBox'
import { Screenshot, Diagram } from './components/mdx/Screenshot'
import { YouTubeVideo } from './components/mdx/YouTubeVideo'
import { LoomVideo } from './components/mdx/LoomVideo'
import { ProductVideo } from './components/mdx/ProductVideo'
import { CommunitySupportedDriver } from './components/mdx/CommunitySupportedDriver'
import { Grid } from './components/mdx/Grid'
import { GridItem } from './components/mdx/GridItem'
import { CodeTabs } from './components/mdx/CodeTabs'
import { Pre } from './components/mdx/Pre'
import { EnvVar } from './components/mdx/EnvVar'
import { FeedbackBlock } from './components/FeedbackBlock'

const themeComponents = getThemeComponents()

// Custom wrapper that adds FeedbackBlock at the bottom of each page
const ThemeWrapper = themeComponents.wrapper
const CustomWrapper = ({ children, ...props }) => (
  <ThemeWrapper {...props}>
    {children}
    <FeedbackBlock />
  </ThemeWrapper>
)

// Stub component that renders children
const Stub = ({ children }) => <>{children}</>

// Stub component that renders nothing
const EmptyStub = () => null

// Stub components - to be replaced with real implementations
const customComponents = {
  // UI Components
  Btn,
  Screenshot,
  Diagram,

  // Code display
  CodeTabs,
  pre: Pre,

  // Alert boxes
  InfoBox,
  WarningBox,
  SuccessBox,
  ReferenceBox,

  // Layout
  Grid,
  GridItem,

  // Video embeds
  YouTubeVideo,
  LoomVideo,
  ProductVideo,

  // Cube-specific components
  CommunitySupportedDriver,
  EnvVar,
  QueryBuilder: EmptyStub,
  QueryRenderer: EmptyStub,
  CubeProvider: Stub,
}

export function useMDXComponents(components) {
  return {
    ...themeComponents,
    ...customComponents,
    wrapper: CustomWrapper,
    ...components
  }
}
