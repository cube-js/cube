import { ReactNode } from 'react';
import { Tabs, Alert, Checkbox, Typography, Space } from 'antd';
import styled from 'styled-components';
import { CodeSnippet } from '../../atoms';
import { CopiableInput } from '../../components/CopiableInput';

import deepnoteSvg from '../../img/bi/deepnote.svg';
import excelSvg from '../../img/bi/excel.svg';
import googleStudioSvg from '../../img/bi/google-data-studio.svg';
import googleSheetsSvg from '../../img/bi/google-sheets.svg';
import jupyterSvg from '../../img/bi/jupyter.svg';
import hexSvg from '../../img/bi/hex.svg';
import metabaseSvg from '../../img/bi/metabase.svg';
import observableSvg from '../../img/bi/observable.svg';
import powerbiSvg from '../../img/bi/power-bi.svg';
import streamlitSvg from '../../img/bi/streamlit.svg';
import supersetSvg from '../../img/bi/superset.svg';
import tableauSvg from '../../img/bi/tableau.svg';
import hightouchSvg from '../../img/bi/hightouch.svg';
import thoughtSpot from '../../img/bi/thoughtspot.svg';
import { Content, Header } from '../components/Ui';

const { Paragraph, Link } = Typography;

const SpaceFlex = styled(Space)`
  div.ant-space {
    display: flex;
  }
`;

type CubeSqlCredentials = {
  id: number;
  cubeSqlHost: string;
  cubeSqlRoute: string;
  cubeSqlUser: string;
  cubeSqlPassword: string;
};

const BI_KEYS = {
  Generic: 'BIs and Visualization Tools',
  Superset: 'Apache Superset',
  Metabase: 'Metabase',
  Tableau: 'Tableau',
  ThoughtSpot: 'ThoughtSpot',
  PowerBI: 'Power BI',
  Hex: 'Hex',
  Jupyter: 'Jupyter notebook',
  Streamlit: 'Streamlit',
  Observable: 'Observable',
  // Not listed on the integration page:
  Deepnote: 'Deepnote',
  Excel: 'Excel',
  GoogleStudio: 'Google Data Studio',
  GoogleSheets: 'Google Sheets',
  Hightouch: 'Hightouch',
} as const;

type BiKeyNames = keyof typeof BI_KEYS;

const IconSection = styled.div`
  width: 32px;
  height: 32px;
  display: flex;
  align-items: center;
  justify-content: center;
  //margin: 1bw;
`;

const cubeIcon = (
  <svg
    viewBox="0 0 14 14"
    width={22}
    height={22}
    xmlns="http://www.w3.org/2000/svg"
  >
    <path
      fill="currentColor"
      fillRule="evenodd"
      clipRule="evenodd"
      d="M14 2.72195V10.5212C14 10.9354 13.7478 11.308 13.3631 11.4618L7 14.0071L0.636881 11.4618C0.252227 11.308 1.556e-07 10.9354 1.556e-07 10.5212V2.72195L6.62372 0.0724644C6.86527 -0.0241549 7.13473 -0.0241547 7.37628 0.0724644L14 2.72195ZM11.9594 3.0961L7 1.11236L2.04064 3.0961L7 5.07985L11.9594 3.0961ZM6.44737 6.0492L1.10526 3.91236V10.4588L6.44737 12.5956L6.44737 6.0492ZM7.55263 12.5956L7.55263 6.0492L12.8947 3.91236V10.4588L7.55263 12.5956Z"
    />
  </svg>
);

const BI_ICONS = {
  Generic: null,
  Deepnote: deepnoteSvg,
  Excel: excelSvg,
  GoogleStudio: googleStudioSvg,
  GoogleSheets: googleSheetsSvg,
  Jupyter: jupyterSvg,
  Hex: hexSvg,
  Hightouch: hightouchSvg,
  Metabase: metabaseSvg,
  Observable: observableSvg,
  PowerBI: powerbiSvg,
  Streamlit: streamlitSvg,
  Superset: supersetSvg,
  Tableau: tableauSvg,
  ThoughtSpot: thoughtSpot,
} as const;

type BiIconNames = keyof typeof BI_ICONS;

type BiIconProps = {
  type: BiIconNames;
};

function BiIcon({ type }: BiIconProps) {
  const src = BI_ICONS[type];
  const key = BI_KEYS[type];

  if (src) {
    return (
      <IconSection>
        <img src={src} width={32} alt={`${key} Icon`} />
      </IconSection>
    );
  }

  return <IconSection>{cubeIcon}</IconSection>;
}

type FieldProps =
  | {
      type: 'custom';
      value:
        | ReactNode
        | ((deployment: CubeSqlCredentials, branchName?: string) => ReactNode);
    }
  | {
      type: 'alert' | 'heading';
      value:
        | string
        | ((deployment: CubeSqlCredentials, branchName?: string) => string);
    }
  | {
      type?: 'snippet' | 'link';
      label: string;
      value:
        | string
        | ((deployment: CubeSqlCredentials, branchName?: string) => string);
    }
  | {
      type?: 'text';
      label: string;
      value:
        | ReactNode
        | ((deployment: CubeSqlCredentials, branchName?: string) => ReactNode);
    }
  | {
      type: 'checkbox';
      label: string;
      value:
        | boolean
        | ((deployment: CubeSqlCredentials, branchName?: string) => boolean);
    };

const CUBESQL_PG_PORT = '15432';

const PG_SNIPPET_FIELD: FieldProps = {
  type: 'snippet',
  label: 'Psql connection string',
  value: ({
    cubeSqlPassword,
    cubeSqlUser,
    cubeSqlHost,
    cubeSqlRoute,
  }: CubeSqlCredentials) => `${
    cubeSqlPassword ? 'PGPASSWORD=' + cubeSqlPassword + ' \\\n  ' : ''
  }psql -h ${cubeSqlHost} \\
  -p ${CUBESQL_PG_PORT} \\
  -U ${cubeSqlUser} ${cubeSqlRoute}`,
};

const POSTGRESQL_FIELD: FieldProps = {
  type: 'text',
  label: 'Connection type',
  value: 'PostgreSQL',
};

const BASE_CREDENTIALS: FieldProps[] = [
  {
    label: 'Host',
    value: ({ cubeSqlHost }) => cubeSqlHost,
  },
  {
    label: 'Port',
    value: CUBESQL_PG_PORT,
  },
  {
    label: 'Database',
    value: ({ cubeSqlRoute }, branchName) =>
      `${cubeSqlRoute}${branchName ? '_' + branchName : ''}`,
  },
  {
    label: 'User',
    value: ({ cubeSqlUser }) => cubeSqlUser,
  },
  {
    label: 'Password',
    value: ({ cubeSqlPassword }) => cubeSqlPassword,
  },
];

type FieldItemProps = {
  label: string;
  children: ReactNode;
};

function Field({ label, children }: FieldItemProps) {
  return (
    <Space direction="vertical">
      <Typography.Text strong>{label}</Typography.Text>

      {children}
    </Space>
  );
}

function getFields(fields: FieldProps[], credentials: CubeSqlCredentials) {
  return fields.map((field) => {
    const value =
      typeof field.value === 'function'
        ? field.value(credentials)
        : field.value;

    switch (field.type) {
      case 'checkbox':
        return (
          <Field key={field.label} label={field.label}>
            <Checkbox checked={!!value} />
          </Field>
        );
      case 'text':
        return (
          <Field label={field.label}>
            <Typography.Text>{value}</Typography.Text>
          </Field>
        );
      case 'link':
        const target = value.startsWith('!') ? '_blank' : undefined;
        const href = value.replace(/^!/, '');

        return (
          <Typography.Paragraph>
            <Typography.Link href={href} target={target}>
              {field.label}↗
            </Typography.Link>
          </Typography.Paragraph>
        );
      case 'alert':
        return <Alert message={field.value} />;
      case 'heading':
        return <Typography.Title>{field.value}</Typography.Title>;
      case 'custom':
        return field.value;
      case 'snippet':
        return <CodeSnippet theme="light" code={value} />;
      default:
        return (
          <Field key={field.label} label={field.label}>
            <CopiableInput value={value as string} title="Server" />
          </Field>
        );
    }
  }, {});
}

function renameFields(
  fields: FieldProps[],
  renameMap: { [key: string]: string }
) {
  return fields.map((field) => {
    const label = (field as { label?: string }).label;

    return {
      ...field,
      ...(label
        ? {
            label: renameMap[label] || label,
          }
        : null),
    };
  });
}

type BIFields = {
  [key in BiKeyNames]: FieldProps[];
};

const BI_FIELDS: BIFields = {
  Generic: [
    POSTGRESQL_FIELD,
    PG_SNIPPET_FIELD,
    ...BASE_CREDENTIALS,
  ],

  PowerBI: [
    POSTGRESQL_FIELD,
    ...renameFields(BASE_CREDENTIALS, {
      Host: 'Server',
      User: 'User name',
    }),
    {
      type: 'text',
      label: 'Data Connectivity mode',
      value: 'DirectQuery',
    },
  ],

  Jupyter: [
    {
      type: 'link',
      label: 'Tutorial: Using Jupyter with Cube',
      value: '!https://cube.dev/docs/config/downstream/jupyter',
    },
    {
      label: 'Driver Name',
      value: 'postgresql',
    },
    ...renameFields(BASE_CREDENTIALS, {
      User: 'Username',
    }),
  ],

  Metabase: [
    {
      type: 'link',
      label: 'Tutorial: Using Metabase with Cube',
      value: '!https://cube.dev/docs/config/downstream/metabase',
    },
    {
      type: 'text',
      label: 'Database Type',
      value: 'PostgreSQL',
    },
    ...renameFields(BASE_CREDENTIALS, {
      Database: 'Database name',
      User: 'Username',
    }),
  ],

  Streamlit: [
    {
      type: 'link',
      label: 'Tutorial: Using Streamlit with Cube',
      value: '!https://cube.dev/docs/config/downstream/streamlit',
    },
    {
      label: 'Driver Name',
      value: 'postgresql',
    },
    ...renameFields(BASE_CREDENTIALS, {
      User: 'Username',
    }),
  ],

  Observable: [
    {
      type: 'link',
      label: 'Tutorial: Using Observable with Cube',
      value: '!https://cube.dev/docs/config/downstream/observable',
    },
    POSTGRESQL_FIELD,
    ...BASE_CREDENTIALS,
    {
      type: 'checkbox',
      label: 'Require SSL/TLS',
      value: true,
    },
  ],

  Tableau: [
    {
      type: 'link',
      label: 'Tutorial: Using Tableau with Cube',
      value: '!https://cube.dev/docs/config/downstream/tableau',
    },
    POSTGRESQL_FIELD,
    ...renameFields(BASE_CREDENTIALS, {
      Host: 'Server',
      User: 'Username',
    }),
    {
      type: 'checkbox',
      label: 'Use SSL',
      value: true,
    },
  ],

  Superset: [
    {
      type: 'link',
      label: 'Tutorial: Using Apache Superset with Cube',
      value:
        '!https://cube.dev/docs/recipes/using-apache-superset-with-cube-sql',
    },
    POSTGRESQL_FIELD,
    ...BASE_CREDENTIALS,
  ],

  GoogleSheets: [
    {
      type: 'link',
      label: 'Connect Cube and Google Sheets using Skyvia',
      value: '!https://skyvia.com/connectors/google-sheets',
    },
    POSTGRESQL_FIELD,
    ...renameFields(BASE_CREDENTIALS, {
      User: 'User ID',
    }),
  ],

  GoogleStudio: [POSTGRESQL_FIELD, ...BASE_CREDENTIALS],

  Excel: [
    {
      type: 'link',
      label: 'Connect Cube and Excel using Devart',
      value: '!https://www.devart.com/excel-addins/postgresql/',
    },
    POSTGRESQL_FIELD,
    ...renameFields(BASE_CREDENTIALS, {
      User: 'User id',
    }),
  ],

  Hex: [
    {
      type: 'link',
      label: 'Tutorial: Using Hex with Cube',
      value: '!https://cube.dev/docs/config/downstream/hex',
    },
    {
      label: 'Lang',
      value: 'Python + SQL',
    },
    POSTGRESQL_FIELD,
    ...renameFields(BASE_CREDENTIALS, {
      User: 'Username',
    }),
    {
      label: 'Type',
      value: 'Password',
    },
  ],

  Hightouch: [POSTGRESQL_FIELD, ...BASE_CREDENTIALS],

  Deepnote: [
    {
      type: 'link',
      label: 'Tutorial: Using Deepnote with Cube',
      value: '!https://cube.dev/docs/config/downstream/deepnote',
    },
    POSTGRESQL_FIELD,
    ...renameFields(BASE_CREDENTIALS, {
      Host: 'Hostname',
      User: 'Username',
    }),
  ],

  ThoughtSpot: [
    {
      type: 'text',
      label: 'Instructions',
      value: (
        <>
          <ul>
            <li>
              Choose the <b>Data</b> tab.
            </li>
            <li>
              Click the <b>Create new</b> button and select <b>Connection</b>.
            </li>
            <li>
              Name your connection and choose <b>Amazon Redshift</b> data
              warehouse.
            </li>
            <li>
              Click <b>Continue</b>.
            </li>
            <li>Fill the all the required data with the below values.</li>
            <li>
              Click <b>Advanced Config</b> and add the key <b>ssl</b> with the
              value <b>false</b>.
            </li>
            <li>
              Click <b>Continue</b>.
            </li>
          </ul>
        </>
      ),
    },
    // {
    //   label: 'Data Workspace',
    //   value: 'Connection',
    // },
    // {
    //   label: 'Data Warehouse',
    //   value: 'Amazon Redshift',
    // },
    ...BASE_CREDENTIALS,
    {
      type: 'heading',
      value: 'Additional config',
    },
    {
      label: 'ssl',
      value: 'false',
    },
  ],
};

export function ConnectToBiPage() {
  const cubeSqlCredentials = {
    id: 1,
    cubeSqlHost: 'localhost',
    cubeSqlRoute: 'test',
    cubeSqlUser: 'username',
    cubeSqlPassword: 'password',
  };

  return (
    <>
      <Header>
        <Typography.Title>Connect to BI</Typography.Title>
      </Header>

      <Content>
        <Paragraph>
          With Cube SQL API you can query Cube via Postgres-compatible SQL.
          It enables the use of BI applications and other visualization tools on top of Cube. <br />
          <Link href="https://cube.dev/docs/config/downstream" target="_blank">Learn more about SQL API and connecting to BI tools in Cube docs ↗ </Link>
        </Paragraph>
        <Tabs defaultActiveKey="1" tabPosition="left" size="small">
          {Object.entries(BI_KEYS).map(([key, title]) => (
            <Tabs.TabPane
              key={key}
              tab={
                <Space>
                  <BiIcon type={key as any} />

                  {title}
                </Space>
              }
            >
              <SpaceFlex
                direction="vertical"
                style={{
                  minWidth: 600,
                  padding: '20px 15px',
                }}
              >
                {getFields(BI_FIELDS[key], cubeSqlCredentials)}
              </SpaceFlex>
            </Tabs.TabPane>
          ))}
        </Tabs>
      </Content>
    </>
  );
}
