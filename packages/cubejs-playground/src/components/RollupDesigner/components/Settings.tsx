import { QuestionCircleFilled } from '@ant-design/icons';
import { GRANULARITIES, TimeDimensionGranularity } from '@cubejs-client/core';
import {
  Card,
  Checkbox,
  Col,
  DatePicker,
  Form,
  FormItemProps,
  Input,
  Radio,
  Row,
  Select,
  Space,
  Tooltip,
  Typography,
} from 'antd';
import { isValidCron } from 'cron-validator';
import { useMemo, useState } from 'react';
import styled from 'styled-components';

import { Flex } from '../../../grid';
import { ucfirst } from '../../../shared/helpers';
import { flatten } from '../utils';

const Wrapper = styled.div`
  display: flex;
  gap: 32px;
  flex-direction: column;
`;

const partionGranularities = GRANULARITIES.filter(
  ({ name }) => !['second', 'minute'].includes(name || '')
).map((granularity) => {
  if (!granularity.name) {
    return {
      ...granularity,
      title: 'No partition',
    };
  }

  return granularity;
});

type BuildRange = {
  sql: string;
};

type RollupIndexColumns = {
  columns: string[];
};

type RefreshKey = {
  every?: string;
  incremental?: boolean;
  updateWindow?: string;
  sql?: string;
};

export type RollupSettings = {
  refreshKey?: RefreshKey;
  partitionGranularity?: any;
  buildRangeStart?: BuildRange;
  buildRangeEnd?: BuildRange;
  indexes?: Record<string, RollupIndexColumns>;
};

type SettingsProps = {
  hasTimeDimension: boolean;
  members: string[];
  onCronExpressionValidityChange: (valid: boolean) => void;
  onChange: (values: Record<string, string>) => void;
};

export function Settings({
  members,
  hasTimeDimension,
  onCronExpressionValidityChange,
  onChange,
}: SettingsProps) {
  const [form] = Form.useForm();
  const initialValues = {
    refreshKey: {
      checked: {
        every: true,
        sql: false,
      },
      sql: '',
      value: 1,
      granularity: 'day',
      cron: '',
    },
    partitionGranularity: '',
    updateWindow: {
      value: 7,
      granularity: 'day',
    },
    incrementalRefresh: true,
    buildRange: {
      since: {
        option: 'relative',
        fixedDate: undefined,
        number: 1,
        granularity: 'year',
        time: 'ago',
      },
      until: {
        option: 'relative',
        fixedDate: undefined,
        number: 1,
        granularity: 'year',
        time: 'from now',
      },
    },
    automatedRefresh: true,
  };

  const flattenedValues = useMemo(() => {
    const values = flatten(initialValues);

    onChange(values);

    return values;
  }, []);

  const [values, setValues] = useState<Record<string, string>>(flattenedValues);

  return (
    <Form
      form={form}
      validateTrigger="onBlur"
      initialValues={flatten(initialValues)}
      onValuesChange={(values) => {
        setValues((prevValues) => {
          onChange({ ...prevValues, ...values });

          Object.keys(values).forEach((field) => {
            const error = form.getFieldError(field);

            if (!error.length) {
              return;
            }

            form.setFields([
              {
                name: field,
                errors: [],
              },
            ]);
          });

          return { ...prevValues, ...values };
        });
      }}
    >
      <Wrapper>
        <Card>
          <TitleWithTooltip title="Refresh Key">
            Specify how often to refresh your pre-aggregated data
          </TitleWithTooltip>

          <Row gutter={8} wrap={false} align="middle">
            <Col flex="85px">
              <Form.Item
                name="refreshKey.checked.every"
                valuePropName="checked"
              >
                <Checkbox>Every</Checkbox>
              </Form.Item>
            </Col>

            <Col flex="auto">
              <Space align="center">
                <Form.Item name="refreshKey.value">
                  <Input
                    disabled={!values['refreshKey.checked.every']}
                    type="number"
                    min={0}
                    style={{ maxWidth: 80 }}
                  />
                </Form.Item>

                <GranularitySelect
                  disabled={!values['refreshKey.checked.every']}
                  name="refreshKey.granularity"
                />

                <div style={{ marginBottom: 24 }}>
                  <Typography.Text>or</Typography.Text>
                </div>

                <Form.Item
                  name="refreshKey.cron"
                  rules={[
                    {
                      validator: (_, value, callback) => {
                        if (value && !isValidCron(value, { seconds: true })) {
                          onCronExpressionValidityChange(false);
                          callback('Cron expression is invalid');
                        } else {
                          onCronExpressionValidityChange(true);
                        }
                      },
                    },
                  ]}
                >
                  <Input
                    allowClear
                    placeholder="Cron Expression"
                    disabled={!values['refreshKey.checked.every']}
                    style={{ maxWidth: 200 }}
                  />
                </Form.Item>
              </Space>
            </Col>
          </Row>

          <Row gutter={8}>
            <Col flex="85px">
              <Form.Item
                name="refreshKey.checked.sql"
                valuePropName="checked"
                noStyle
              >
                <Checkbox>SQL</Checkbox>
              </Form.Item>
            </Col>

            <Col flex="auto">
              <Form.Item name="refreshKey.sql" noStyle>
                <Input.TextArea
                  disabled={!values['refreshKey.checked.sql']}
                  placeholder="SELECT MAX(createdAt) FROM orders"
                />
              </Form.Item>
            </Col>
          </Row>

          {/* <Form.Item
            name="automatedRefresh"
            valuePropName="checked"
            noStyle
          >
            <Checkbox>Automated Refresh</Checkbox>
          </Form.Item> */}
        </Card>

        <Card>
          {hasTimeDimension ? (
            <>
              <TitleWithTooltip title="Partition Granularity">
                Partitions are shards of the pre-aggregation dataset. To enable
                partitions, you must specify here the desired granularity.
              </TitleWithTooltip>

              <Form.Item name="partitionGranularity">
                <Select showSearch style={{ maxWidth: 150 }}>
                  {partionGranularities.map(({ name, title }) => (
                    <Select.Option key={name} value={name || ''}>
                      {title}
                    </Select.Option>
                  ))}
                </Select>
              </Form.Item>

              {values.partitionGranularity ? (
                <>
                  <Form.Item name="incrementalRefresh" valuePropName="checked">
                    <Checkbox>Incremental Refresh</Checkbox>
                  </Form.Item>

                  <TitleWithTooltip title="Update Window">
                    Any partition which includes this span of time into the past
                    from now will be refreshed according to the Refresh Key set
                    above. Otherwise, if left unset, only the most recent
                    partition will be refreshed regularly.
                  </TitleWithTooltip>

                  <Space align="start">
                    <Form.Item name="updateWindow.value">
                      <Input type="number" min={0} style={{ maxWidth: 80 }} />
                    </Form.Item>

                    <GranularitySelect
                      name="updateWindow.granularity"
                      excludedGranularities={['second']}
                    />
                  </Space>
                  {/* <Typography.Paragraph strong>Build Range</Typography.Paragraph> */}
                  {/* <Flex direction="column" gap={4}>
  <BuildRange time="since" />

  <BuildRange time="until" />
</Flex> */}
                </>
              ) : null}
            </>
          ) : null}
          <Typography.Paragraph strong>Indexes</Typography.Paragraph>

          <Form.Item name="indexes" noStyle>
            <Select
              mode="tags"
              style={{ width: '100%' }}
              placeholder="(list column names)"
            >
              {members.map((name) => (
                <Select.Option value={name}>{name}</Select.Option>
              ))}
            </Select>
          </Form.Item>
        </Card>
      </Wrapper>
    </Form>
  );
}

type BuildRangeProps = {
  time: string;
};

function BuildRange({ time }: BuildRangeProps) {
  const name = (key) => `buildRange.${time}.${key}`;

  return (
    <Row>
      <Col flex="60px">{ucfirst(time)}</Col>

      <Col flex="auto">
        <Form.Item name={`buildRange.${time}.option`} noStyle>
          <Radio.Group>
            <Flex direction="column" gap={2}>
              <Space>
                <Radio value="relative" />

                <Form.Item name={name('number')} noStyle>
                  <Input type="number" min={0} style={{ maxWidth: 80 }} />
                </Form.Item>

                <GranularitySelect name={name('granularity')} noStyle />
              </Space>

              <Space>
                <Radio value="fixed" />

                <Form.Item name={name('fixedDate')} noStyle>
                  <DatePicker
                    placeholder="Fixed date"
                    style={{ width: '100%' }}
                  />
                </Form.Item>
              </Space>

              <Radio value="now">Now</Radio>
            </Flex>
          </Radio.Group>
        </Form.Item>
      </Col>
    </Row>
  );
}

type GranularitySelectProps = {
  excludedGranularities?: TimeDimensionGranularity[];
  disabled?: boolean;
};

function GranularitySelect({
  disabled,
  excludedGranularities = [],
  ...props
}: FormItemProps & GranularitySelectProps) {
  return (
    <Form.Item {...props}>
      <Select disabled={disabled} showSearch style={{ minWidth: 100 }}>
        {GRANULARITIES.filter(
          ({ name }) => name != null && !excludedGranularities.includes(name)
        ).map(({ name, title }) => (
          <Select.Option key={name} value={name as string}>
            {title}
          </Select.Option>
        ))}
      </Select>
    </Form.Item>
  );
}

type TitleWithTooltipProps = {
  title: string;
  children: string;
};

function TitleWithTooltip({ title, children }: TitleWithTooltipProps) {
  return (
    <Space
      align="baseline"
      style={{
        display: 'flex',
      }}
    >
      <Typography.Paragraph strong>{title}</Typography.Paragraph>
      <Tooltip title={children}>
        <QuestionCircleFilled style={{ color: '#1414464D' }} />
      </Tooltip>
    </Space>
  );
}
