import {
  Alert,
  Checkbox,
  Divider,
  Form,
  Grid,
  Radio,
  Space,
  useForm,
} from '@cube-dev/ui-kit';
import { Query } from '@cubejs-client/core';
import { useCallback, useState } from 'react';

import { AllParams } from './types';
import { ALL_VIZARD_OPTIONS, VIZARD_OPTIONS } from './options';
import { getAvailableOptions, validateVisualParams } from './helpers';

export interface VizardSetupProps {
  apiToken: string | null;
  apiUrl: string | null;
  query: Query;
  data: AllParams;
  onChange: (params: AllParams) => void;
}

export function Setup(props: VizardSetupProps) {
  const { onChange, data } = props;
  const [form] = useForm<AllParams>();
  const [availableProps, setAvailableProps] = useState(
    getAvailableOptions(data)
  );

  const onValuesChange = useCallback((data: AllParams) => {
    const { visualization, framework, language, library } = data;
    const visualParams = { visualization, framework, language, library };
    const newVisualParams = validateVisualParams(visualParams);

    if (JSON.stringify(newVisualParams) !== JSON.stringify(visualParams)) {
      form.setFieldsValue(newVisualParams);
    }

    setAvailableProps(getAvailableOptions(newVisualParams));
    onChange?.({
      ...newVisualParams,
      useWebSockets: data.useWebSockets,
      useSubscription: data.useSubscription,
    });
  }, []);

  return (
    <Grid styles={{ overflow: 'auto', padding: '1x 2x', height: 'max 100vh' }}>
      <Form form={form} defaultValues={data} onValuesChange={onValuesChange}>
        {/*<Title level={2} preset="h3">*/}
        {/*  Visualization*/}
        {/*</Title>*/}
        <Radio.Group
          name="visualization"
          label="Visualization"
          orientation="horizontal"
          groupStyles={{
            display: 'grid',
            gridTemplateColumns: 'repeat(3, 1fr)',
            placeContent: 'stretch',
            placeItems: 'stretch',
          }}
        >
          {ALL_VIZARD_OPTIONS.visualization.map((option) => {
            const { icon, name } = VIZARD_OPTIONS[option];

            return (
              <Radio.Button
                key={option}
                value={option}
                styles={{
                  placeSelf: 'stretch',
                  placeItems: 'stretch',
                  width: 'initial',
                }}
              >
                {icon ? (
                  <Space gap="1x">
                    {icon}
                    <div>{name}</div>
                  </Space>
                ) : (
                  name
                )}
              </Radio.Button>
            );
          })}
        </Radio.Group>
        {/*<Title level={2} preset="h3">*/}
        {/*  Stack*/}
        {/*</Title>*/}
        <Radio.Group
          name="framework"
          label="Framework"
          orientation="horizontal"
        >
          {availableProps?.framework.map((framework) => {
            const { icon, name } = VIZARD_OPTIONS[framework];

            return (
              <Radio.Button key={framework} value={framework}>
                {icon ? (
                  <Space gap="1x">
                    {icon}
                    <div>{name}</div>
                  </Space>
                ) : (
                  name
                )}
              </Radio.Button>
            );
          })}
        </Radio.Group>
        {availableProps?.language?.length &&
        availableProps.language.length > 1 ? (
          <Radio.ButtonGroup
            name="language"
            label="Language"
            orientation="horizontal"
          >
            {availableProps?.language.map((language) => {
              const { icon, name } = VIZARD_OPTIONS[language];

              return (
                <Radio.Button key={language} value={language}>
                  {icon ? (
                    <Space gap="1x">
                      {icon}
                      <div>{name}</div>
                    </Space>
                  ) : (
                    name
                  )}
                </Radio.Button>
              );
            })}
          </Radio.ButtonGroup>
        ) : null}
        {availableProps?.library.length ? (
          <Radio.Group name="library" label="Library" orientation="horizontal">
            {availableProps?.library.map((library) => {
              return (
                <Radio.Button key={library} value={library}>
                  {VIZARD_OPTIONS[library].name}
                </Radio.Button>
              );
            })}
          </Radio.Group>
        ) : (
          <Alert>
            This combination of options supports no charting library <u>yet</u>.
          </Alert>
        )}

        {/*<Title level={2} preset="h3">*/}
        {/*  API Options*/}
        {/*</Title>*/}

        {/*<Radio.ButtonGroup*/}
        {/*  name="apiType"*/}
        {/*  label="Type"*/}
        {/*  orientation="horizontal"*/}
        {/*  styles={{ '--label-width': '50px' }}*/}
        {/*>*/}
        {/*  <Radio.Button value="rest">REST API</Radio.Button>*/}
        {/*  <Radio.Button value="graphql">GraphQL API</Radio.Button>*/}
        {/*</Radio.ButtonGroup>*/}
        <Divider />
        <Checkbox name="useWebSockets">Use WebSockets</Checkbox>
        <Checkbox name="useSubscription">Use Subscription</Checkbox>
      </Form>
      {/*<Flow gap="2x">*/}
      {/*  <Field label="Cube API URL">*/}
      {/*    <CopySnippet showScroll code={apiUrl || ''} />*/}
      {/*  </Field>*/}
      {/*  <Field label="Cube API Token">*/}
      {/*    <CopySnippet showScroll code={apiToken || ''} />*/}
      {/*  </Field>*/}
      {/*  <Field label="Query">*/}
      {/*    <CopySnippet showScroll code={JSON.stringify(query, null, 2) || ''} />*/}
      {/*  </Field>*/}
      {/*</Flow>*/}
      {/*<Block border="top" padding="1x">*/}
      {/*  <Button></Button>*/}
      {/*</Block>*/}
    </Grid>
  );
}
