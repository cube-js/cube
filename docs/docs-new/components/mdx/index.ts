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
import { Screenshot } from '@/components/mdx/Screenshot';
import { YouTubeVideo } from '@/components/mdx/YouTubeVideo/YouTubeVideo';
import { CloudPromoBlock } from '@/components/mdx/CloudPromoBlock/CloudPromoBlock';
import { EventPromoBlock } from '@/components/mdx/EventPromoBlock/EventPromoBlock';

export const components = {
  ...Buttons,
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
  YouTubeVideo
};
