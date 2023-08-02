import * as Buttons from '@/components/common/Button/Button';
import { CodeTabs } from "@/components/mdx/CodeTabs";
import { CubeQueryResultSet } from "@/components/mdx/CubeQueryResultSet";
import {
  DangerBox,
  InfoBox,
  SuccessBox,
  WarningBox,
} from "@/components/mdx/AlertBox/AlertBox";
import { GitHubCodeBlock } from "@/components/mdx/GitHubCodeBlock";
import { GitHubFolderLink } from "@/components/mdx/GitHubFolderLink";
import { Grid } from "@/components/mdx/Grid/Grid";
import { GridItem } from "@/components/mdx/Grid/GridItem";
import { InlineButton } from "@/components/mdx/InlineButton/InlineButton";
import { LoomVideo } from "@/components/mdx/LoomVideo/LoomVideo";
import { ParameterTable } from "@/components/mdx/ReferenceDocs/ParameterTable";
import { Snippet, SnippetGroup } from "@/components/mdx/Snippets/SnippetGroup";
import { Diagram, Screenshot } from '@/components/mdx/Screenshot';
import { YouTubeVideo } from '@/components/mdx/YouTubeVideo/YouTubeVideo';
import { CaseStudyPromoBlock } from '@/components/mdx/CaseStudyPromoBlock/CaseStudyPromoBlock';
import { CloudPromoBlock } from '@/components/mdx/CloudPromoBlock/CloudPromoBlock';
import { EventPromoBlock } from '@/components/mdx/EventPromoBlock/EventPromoBlock';
import { H1 } from '@/components/overrides/Headings/H1';
import { Link } from '../overrides/Anchor/Link';
import { Table } from '@/components/overrides/Table/Table';
import { Td } from '@/components/overrides/Table/Td';
import { Th } from '@/components/overrides/Table/Th';
import { Tr } from '@/components/overrides/Table/Tr';

export const components = {
  ...Buttons,
  CaseStudyPromoBlock,
  CloudPromoBlock,
  EventPromoBlock,
  DangerBox,
  InfoBox,
  SuccessBox,
  WarningBox,
  LoomVideo,
  Grid,
  GridItem,
  GitHubCodeBlock,
  CubeQueryResultSet,
  GitHubFolderLink,
  ParameterTable,
  SnippetGroup,
  Snippet,
  // h2: ScrollSpyH2,
  // h3: ScrollSpyH3,
  // h4: MyH4,
  CodeTabs,
  Btn: InlineButton,
  Screenshot,
  Diagram,
  YouTubeVideo,

  // Overrides
  h1: H1,
  a: Link,
  table: Table,
  td: Td,
  th: Th,
  tr: Tr,
};
