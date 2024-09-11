import { ReactNode } from 'react';
import {
  AreaChartIcon,
  BarChartIcon,
  DonutIcon,
  LineChartIcon,
  PieChartIcon,
  TableIcon,
  tasty,
} from '@cube-dev/ui-kit';

import stats from './stats.json';

import { APP_OPTIONS } from './app-options';

const Image = tasty({
  as: 'img',
  styles: {
    display: 'block',
    width: '16px',
    height: '16px',
  },
});

export const ALL_VIZARD_OPTIONS = APP_OPTIONS;

export const VIZARD_PARAMS_MAP = stats as unknown as {
  [key in (typeof ALL_VIZARD_OPTIONS)['visualization'][number]]?: {
    [key in (typeof ALL_VIZARD_OPTIONS)['framework'][number]]?: {
      [key in (typeof ALL_VIZARD_OPTIONS)['language'][number]]?: {
        [key in (typeof ALL_VIZARD_OPTIONS)['library'][number]]?: true;
      };
    };
  };
};

export const VIZARD_OPTIONS: {
  [key in (typeof ALL_VIZARD_OPTIONS)[keyof typeof ALL_VIZARD_OPTIONS][number]]: {
    name: string;
    type: 'visualization' | 'framework' | 'language' | 'library';
    icon?: ReactNode;
  };
} = {
  line: {
    name: 'Line',
    type: 'visualization',
    icon: <LineChartIcon />,
  },
  bar: {
    name: 'Bar',
    type: 'visualization',
    icon: <BarChartIcon />,
  },
  area: {
    name: 'Area',
    type: 'visualization',
    icon: <AreaChartIcon />,
  },
  pie: {
    name: 'Pie',
    type: 'visualization',
    icon: <PieChartIcon />,
  },
  doughnut: {
    name: 'Donut',
    type: 'visualization',
    icon: <DonutIcon />,
  },
  react: {
    name: 'React',
    type: 'framework',
    icon: <Image src="./logos/react.svg" />,
  },
  typescript: {
    name: 'TypeScript',
    type: 'language',
    icon: <Image src="./logos/ts.svg" />,
  },
  javascript: {
    name: 'JavaScript',
    type: 'language',
    icon: <Image src="./logos/js.svg" />,
  },
  chartjs: {
    name: 'Chart.js',
    type: 'library',
  },
  table: {
    name: 'Table',
    type: 'library',
    icon: <TableIcon />,
  },
  angular: {
    name: 'Angular',
    type: 'framework',
    icon: <Image src="./logos/angular.svg" />,
  },
  vue: {
    name: 'Vue',
    type: 'framework',
    icon: <Image src="./logos/vue.svg" />,
  },
  antd: {
    name: 'Ant Design',
    type: 'library',
  },
};
