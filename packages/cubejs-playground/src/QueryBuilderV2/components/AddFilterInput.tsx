import {
  Block,
  Button,
  ComboBox,
  Menu,
  MenuTrigger,
  PlusIcon,
  Select,
  tasty,
  useToastsApi,
} from '@cube-dev/ui-kit';
import { Filter, TCubeDimension, TCubeMeasure, TCubeSegment } from '@cubejs-client/core';
import { Key, useEffect, useMemo, useRef, useState } from 'react';

import { useQueryBuilderContext } from '../context';
import { useEvent } from '../hooks';
import { MemberType } from '../types';

import { MemberLabel } from './MemberLabel';

const AddFilterButton = tasty(Button, {
  qa: 'AddFilterButton',
  'aria-label': 'Add a new filter',
  size: 'small',
  type: 'secondary',
  icon: <PlusIcon />,
  width: {
    '': '3x',
    label: 'auto',
  },
});

interface AddFilterInputProps {
  hasLabel?: boolean;
  isCompact?: boolean;
  onAdd: (filter: Filter) => void;
  onSegmentAdd?: (segment: string) => void;
  onDateRangeAdd?: (timeDimension: string) => void;
}

export function AddFilterInput(props: AddFilterInputProps) {
  const { onAdd, onDateRangeAdd, onSegmentAdd, isCompact, hasLabel } = props;
  const [mode, setMode] = useState<'measure' | 'dimension' | 'segment' | 'dateRange' | null>(null);
  const { toast } = useToastsApi();
  const inputRef = useRef<HTMLInputElement>(null);

  const {
    query,
    joinableMembers,
    usedCubes,
    cubes,
    memberViewType,
    dateRanges: queryDateRanges,
  } = useQueryBuilderContext();

  const [members, dimensions, measures, dateRanges, segments, nameToMemberType] = useMemo(() => {
    // Sort members by cube name and used status
    const sort = (
      a: TCubeDimension | TCubeMeasure | TCubeSegment,
      b: TCubeDimension | TCubeMeasure | TCubeSegment
    ) => {
      const aCubeName = usedCubes.find((cubeName) => cubeName === a.name.split('.')[0]);
      const bCubeName = usedCubes.find((cubeName) => cubeName === b.name.split('.')[0]);

      if (aCubeName || bCubeName) {
        if (aCubeName && bCubeName) {
          return aCubeName.localeCompare(bCubeName);
        } else {
          return aCubeName && !bCubeName ? -1 : 1;
        }
      }

      return a.name.localeCompare(b.name);
    };

    const members = [
      ...Object.values(joinableMembers.dimensions).sort(sort),
      ...Object.values(joinableMembers.measures).sort(sort),
      ...Object.values(joinableMembers.segments).sort(sort),
    ];

    const nameToMemberType = members.reduce(
      (acc, member) => {
        acc[member.name] = ['dimension', 'measure', 'segment'][
          [
            !!joinableMembers.dimensions[member.name],
            !!joinableMembers.measures[member.name],
            !!joinableMembers.segments[member.name],
          ].indexOf(true)
        ] as MemberType;

        return acc;
      },
      {} as Record<string, MemberType>
    );

    const dimensions = members.filter((member) => {
      return nameToMemberType[member.name] === 'dimension';
    });

    const measures = members.filter((member) => {
      return nameToMemberType[member.name] === 'measure';
    });

    const segments = members
      .filter((member) => {
        return nameToMemberType[member.name] === 'segment';
      })
      .filter((member) => {
        return !query.segments?.includes(member.name);
      });

    const dateRanges = members
      .filter(
        (member) =>
          nameToMemberType[member.name] === 'dimension' &&
          (member as TCubeDimension).type === 'time'
      )
      .filter((member) => {
        return !queryDateRanges.list.includes(member.name);
      }) as TCubeDimension[];

    return [
      members,
      dimensions as TCubeDimension[],
      measures as TCubeMeasure[],
      dateRanges as TCubeDimension[],
      segments as TCubeSegment[],
      nameToMemberType,
    ];
  }, [
    JSON.stringify(joinableMembers),
    queryDateRanges.list.join(),
    query.segments?.join(),
    usedCubes.length,
  ]);

  const shownMembers = useMemo(() => {
    let shownMembers: (TCubeSegment | TCubeDimension | TCubeMeasure)[];

    switch (mode) {
      case 'measure':
        shownMembers = measures;
        break;
      case 'dimension':
        shownMembers = dimensions;
        break;
      case 'segment':
        shownMembers = segments;
        break;
      case 'dateRange':
        shownMembers = dateRanges;
        break;
      default:
        shownMembers = [];
    }

    return shownMembers;
  }, [members, mode]);

  const onAction = useEvent((key: Key) => {
    if (key === 'and') {
      onAdd({ and: [] as Filter[] });

      return;
    }

    if (key === 'or') {
      onAdd({ or: [] as Filter[] });

      return;
    }

    setMode(key as 'measure' | 'dimension' | 'segment' | 'dateRange');
  });

  const onFilterAdd = useEvent((key: Key | null) => {
    if (!key) {
      return;
    }

    if (mode === 'dateRange') {
      onDateRangeAdd?.(key as string);
    } else if (mode === 'segment') {
      onSegmentAdd?.(key as string);
    } else {
      onAdd({ member: key as string, operator: 'equals', values: [] });
    }
  });

  const items = useMemo(() => {
    const items = [
      { value: 'dimension', label: 'Filter by Dimension' },
      { value: 'measure', label: 'Filter by Measure' },
    ];

    if (onSegmentAdd) {
      items.push({ value: 'segment', label: 'Filter by Segment' });
    }

    if (onDateRangeAdd) {
      items.push({ value: 'dateRange', label: 'Filter by Date Range' });
    }

    items.push({ value: 'and', label: 'AND Branch' }, { value: 'or', label: 'OR Branch' });

    return items;
  }, [onDateRangeAdd, onSegmentAdd]);

  const disabledKeys = useMemo(() => {
    const disabledKeys: string[] = [];

    if (!dateRanges.length) {
      disabledKeys.push('dateRange');
    }

    if (!dimensions.length) {
      disabledKeys.push('dimension');
    }

    if (!measures.length) {
      disabledKeys.push('measure');
    }

    if (!segments.length) {
      disabledKeys.push('segment');
    }

    return disabledKeys;
  }, [dateRanges.length, dimensions.length, measures.length, segments.length]);

  useEffect(() => {
    if (mode && !shownMembers.length && !['or', 'and'].includes(mode)) {
      const title = {
        measure: 'filter',
        dimension: 'filter',
        segment: 'segment',
        dateRange: 'date range',
      }[mode];

      toast.attention({
        header: `Unable to add new ${title}`,
        description: 'No available members',
      });
      setMode(null);
    }
  }, [shownMembers.length, mode]);

  // Hack to focus input after it's shown
  // autoFocus and menuTrigger="focus" don't work together
  useEffect(() => {
    setTimeout(() => {
      inputRef.current?.focus();
    }, 100);
  }, [inputRef.current, mode]);

  return (
    <Block>
      {!mode ? (
        <MenuTrigger>
          <AddFilterButton mods={{ label: hasLabel }}>
            {hasLabel ? 'Add' : undefined}
          </AddFilterButton>
          <Menu items={items} disabledKeys={disabledKeys} onAction={onAction}>
            {items.map((item) => (
              <Menu.Item key={item.value} textValue={item.value}>
                {item.label}
              </Menu.Item>
            ))}
          </Menu>
        </MenuTrigger>
      ) : (
        <ComboBox
          qa="AddFilterInput"
          inputRef={inputRef}
          menuTrigger="focus"
          size="small"
          width="50x"
          placeholder={items.find((item) => item.value === mode)?.label}
          listBoxStyles={{
            height: 'max min(40x, 45vh)',
          }}
          onSelectionChange={onFilterAdd}
          onBlur={() => setMode(null)}
          onKeyDown={(e) => {
            if (e.key === 'Escape') {
              setMode(null);
            }
          }}
        >
          {shownMembers.map((member) => {
            const memberName = member.name.split('.')[1];
            const cubeName = member.name.split('.')[0];
            const cube = cubes.find((cube) => cube.name === cubeName);

            return (
              <Select.Item key={member.name} textValue={member.name}>
                <MemberLabel
                  name={member.name}
                  memberName={memberName}
                  memberTitle={member.shortTitle}
                  cubeName={cubeName}
                  cubeTitle={cube?.title}
                  memberViewType={memberViewType}
                  isCompact={isCompact}
                  type={'type' in member ? (member as TCubeDimension).type : undefined}
                  memberType={mode === 'dateRange' ? 'timeDimension' : mode}
                />
              </Select.Item>
            );
          })}
        </ComboBox>
      )}
    </Block>
  );
}
