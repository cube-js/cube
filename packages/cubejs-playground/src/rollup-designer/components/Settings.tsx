import { QuestionCircleFilled } from '@ant-design/icons';
import { GRANULARITIES, TimeDimensionGranularity } from '@cubejs-client/core';
import {
  Card,
  Checkbox,
  Col,
  DatePicker,
  Form,
  Input,
  Radio,
  Row,
  Select,
  SelectProps,
  Space,
  Tooltip,
  Typography,
} from 'antd';
import { isValidCron } from 'cron-validator';
import { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { Flex } from '../../grid';
import { ucfirst } from '../../shared/helpers';
import { timeZones } from '../../shared/time-zones';
import { flatten } from '../utils';

const Wrapper = styled.div`
  display: flex;
  gap: 32px;
  flex-direction: column;
  padding: 24px;
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
  timezone?: string;
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
  onChange: (values: Record<string, string | boolean>) => void;
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
      isCron: false,
      sql: '',
      value: 1,
      granularity: 'hour',
      cron: '',
      timeZone: undefined,
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
  const [isCron, toggleCron] = useState(false);

  useEffect(() => {
    onChange({ ...values, 'refreshKey.isCron': isCron });

    if (!isCron && !isValidCron(values['refreshKey.cron'])) {
      form.setFields([
        {
          name: 'refreshKey.cron',
          value: '',
          errors: [],
        },
      ]);
    }
  }, [isCron]);

  return (
    <Form
      form={form}
      validateTrigger="onBlur"
      initialValues={flatten(initialValues)}
      onValuesChange={(values) => {
        setValues((prevValues) => {
          onChange({ ...prevValues, ...values, 'refreshKey.isCron': isCron });

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

          <Typography.Paragraph>
            If you do not specify any Refresh Key, refreshes will still default
            to every 1 hour.
          </Typography.Paragraph>

          <Row gutter={8} wrap={false} align="top">
            <Col flex="85px">
              <Form.Item
                name="refreshKey.checked.every"
                valuePropName="checked"
              >
                <Checkbox>Every</Checkbox>
              </Form.Item>
            </Col>

            <Col flex="auto">
              <Flex direction="column" gap={2}>
                <Space>
                  <Radio
                    checked={!isCron}
                    disabled={!values['refreshKey.checked.every']}
                    onClick={() => toggleCron(false)}
                  />

                  <Form.Item name="refreshKey.value" noStyle>
                    <Input
                      data-testid="rd-input-every"
                      disabled={!values['refreshKey.checked.every'] || isCron}
                      type="number"
                      min={0}
                      style={{ maxWidth: 80 }}
                    />
                  </Form.Item>

                  <Form.Item name="refreshKey.granularity" noStyle>
                    <GranularitySelect
                      data-testid="rd-select-every-granularity"
                      disabled={!values['refreshKey.checked.every'] || isCron}
                      excludedGranularities={['year', 'quarter', 'month']}
                    />
                  </Form.Item>
                </Space>

                <Space align="start" style={{ marginBottom: 32 }}>
                  <Radio
                    disabled={!values['refreshKey.checked.every']}
                    checked={isCron}
                    style={{ paddingTop: 5 }}
                    onClick={() => toggleCron(true)}
                  />

                  <Flex direction="column" gap={1}>
                    <Form.Item
                      name="refreshKey.cron"
                      rules={[
                        {
                          validator: (_, value, callback) => {
                            if (
                              value &&
                              !isValidCron(value, { seconds: true })
                            ) {
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
                        placeholder="Cron expression e.g. 30 5 * * 5"
                        disabled={
                          !values['refreshKey.checked.every'] || !isCron
                        }
                      />
                    </Form.Item>

                    {isCron && values['refreshKey.checked.every'] ? (
                      <>
                        <Typography.Paragraph>
                          <Typography.Link
                            target="_blank"
                            href="https://cube.dev/docs/schema/reference/cube#supported-cron-formats"
                          >
                            See how to format your cron expression
                          </Typography.Link>
                        </Typography.Paragraph>

                        <Typography.Paragraph strong>
                          Time Zone
                        </Typography.Paragraph>

                        <Form.Item name="refreshKey.timeZone" noStyle>
                          <Select
                            showSearch
                            style={{ maxWidth: 200 }}
                            placeholder="Select Time Zone"
                          >
                            {timeZones.map((name) => (
                              <Select.Option key={name} value={name || ''}>
                                {name}
                              </Select.Option>
                            ))}
                          </Select>
                        </Form.Item>
                      </>
                    ) : null}
                  </Flex>
                </Space>
              </Flex>
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

                  {values['incrementalRefresh'] && (
                    <>
                      <TitleWithTooltip title="Update Window">
                        Any partition which includes this span of time into the
                        past from now will be refreshed according to the Refresh
                        Key set above. Otherwise, if left unset, only the most
                        recent partition will be refreshed regularly.
                      </TitleWithTooltip>

                      <Space align="start">
                        <Form.Item name="updateWindow.value">
                          <Input
                            type="number"
                            min={0}
                            style={{ maxWidth: 80 }}
                          />
                        </Form.Item>

                        <Form.Item name="updateWindow.granularity">
                          <GranularitySelect
                            excludedGranularities={['second']}
                          />
                        </Form.Item>
                      </Space>
                    </>
                  )}

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
                <Select.Option key={name} value={name}>
                  {name}
                </Select.Option>
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

                <Form.Item name={name('granularity')} noStyle>
                  <GranularitySelect />
                </Form.Item>
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
} & SelectProps<any>;

export function GranularitySelect({
  excludedGranularities = [],
  ...props
}: GranularitySelectProps) {
  return (
    <Select style={{ minWidth: 100 }} showSearch {...props}>
      {GRANULARITIES.filter(
        ({ name }) => name != null && !excludedGranularities.includes(name)
      ).map(({ name, title }) => (
        <Select.Option key={name} value={name as string}>
          {title}
        </Select.Option>
      ))}
    </Select>
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
