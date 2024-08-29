import { Badge, Button, Paragraph } from '@cube-dev/ui-kit';
import { SettingOutlined } from '@ant-design/icons';
import { Meta, StoryFn } from '@storybook/react';
import { useLayoutEffect, useRef } from 'react';

import { Accordion } from './Accordion';
import { AccordionProps } from './types';

export default {
  title: 'Accordion',
  component: Accordion,
  args: {
    children: [
      <Accordion.Item key="1" title="Create Cube">
        <Paragraph>
          Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt
          ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
          ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in
          reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur
          sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id
          est laborum.
        </Paragraph>
      </Accordion.Item>,
      <Accordion.Item key="2" isDefaultExpanded title="Metrics Catalog">
        <Paragraph>
          Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt
          ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
          ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in
          reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur
          sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id
          est laborum.
        </Paragraph>
      </Accordion.Item>,
      <Accordion.Item key="3" title="Generate Data Schema" subtitle={<Badge>12</Badge>}>
        <Paragraph>
          Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt
          ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
          ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in
          reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur
          sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id
          est laborum.
        </Paragraph>
      </Accordion.Item>,
    ],
  },
} as Meta<AccordionProps>;

const Template: StoryFn<AccordionProps> = (args) => <Accordion {...args} />;

export const Default = Template.bind({});

export const Small = Template.bind({});
Small.args = { size: 'small' };

export const Lazy = Template.bind({});
Lazy.args = { isLazy: true };

export const LazyChildren = Template.bind({});
LazyChildren.args = {
  children: (
    <>
      <Accordion.Item title="public.lineItems">
        <Paragraph>Text</Paragraph>
      </Accordion.Item>
      <Accordion.Item title="other">
        <Paragraph>other</Paragraph>
      </Accordion.Item>
    </>
  ),
};

export const WithExtraActions = Template.bind({});
WithExtraActions.args = {
  children: [
    <Accordion.Item
      key="1"
      title="Create Cube"
      extra={
        <Button size="small" type="link">
          Link
        </Button>
      }
    >
      <Paragraph>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut
        labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco
        laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in
        voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat
        cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
      </Paragraph>
    </Accordion.Item>,
    <Accordion.Item
      key="2"
      isDefaultExpanded
      title="Metrics Catalog"
      extra={<Button type="clear" icon={<SettingOutlined />} label="Settings" />}
    >
      <Paragraph>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut
        labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco
        laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in
        voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat
        cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
      </Paragraph>
    </Accordion.Item>,
  ],
};

export const ShowExtraOnHover = Template.bind({});
ShowExtraOnHover.args = {
  children: [
    <Accordion.Item
      key="1"
      title="Create Cube"
      extra={
        <Button size="small" type="link">
          Link
        </Button>
      }
      showExtra="onHover"
    >
      <Paragraph>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut
        labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco
        laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in
        voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat
        cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
      </Paragraph>
    </Accordion.Item>,
    <Accordion.Item
      key="2"
      isDefaultExpanded
      title="Metrics Catalog"
      extra={<Button type="clear" icon={<SettingOutlined />} label="Settings" />}
      showExtra="onHover"
    >
      <Paragraph>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut
        labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco
        laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in
        voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat
        cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
      </Paragraph>
    </Accordion.Item>,
  ],
};

export const AutoChangingHeight: StoryFn<AccordionProps> = (args) => {
  const ref = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    let rafID: number | null = null;

    const id = setInterval(() => {
      rafID = requestAnimationFrame(() => {
        if (ref.current) {
          ref.current.style.height = `${Math.random() * 100}px`;
          rafID = null;
        }
      });
    }, 5000);

    return () => {
      clearInterval(id);

      if (rafID) {
        cancelAnimationFrame(rafID);
      }
    };
  }, []);

  return (
    <Accordion {...args}>
      <Accordion.Item key="1" isDefaultExpanded title="Create Cube">
        <Paragraph ref={ref} fill="#purple_03" height={150}></Paragraph>
      </Accordion.Item>
    </Accordion>
  );
};
