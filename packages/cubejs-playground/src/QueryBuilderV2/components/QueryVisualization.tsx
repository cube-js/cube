import { ReactNode, useMemo } from 'react';
import { Block, Button, Flex, Space, Title, CloseIcon, TooltipProvider } from '@cube-dev/ui-kit';
import {
  CaretDownFilled,
  LoadingOutlined,
  PlusOutlined,
  UnorderedListOutlined,
} from '@ant-design/icons';

import { capitalize } from '../utils/capitalize';
import { useQueryBuilderContext } from '../context';
import { CubeStats } from '../types';

import { MemberLabelText } from './MemberLabelText';
import { ListCube } from './ListCube';
import { MemberBadge } from './Badge';

const TYPES: readonly ('measure' | 'segment' | 'dimension' | 'filter' | 'timeDimension')[] = [
  'measure',
  'dimension',
  'filter',
  'segment',
  'timeDimension',
];

type AllComponentTypes = typeof TYPES;
type ComponentType = AllComponentTypes[number];

const TYPE_PROPS_MAP: Record<ComponentType, Exclude<keyof CubeStats, 'missing' | 'instance'>> = {
  measure: 'measures',
  dimension: 'dimensions',
  filter: 'filters',
  segment: 'segments',
  timeDimension: 'timeDimensions',
};

interface QueryVisualizationProps {
  onReset?: () => void;
  type: 'views' | 'cubes';
  actions?: ReactNode;
}

export function QueryVisualization({
  onReset,
  type: selectedType,
  actions,
}: QueryVisualizationProps) {
  const {
    queryHash,
    isVerifying,
    clearQuery,
    selectedCube,
    selectCube,
    isQueryEmpty,
    members,
    cubes,
    connectionId,
    isCubeUsed,
    joinableCubes,
    queryStats,
  } = useQueryBuilderContext();

  const connectedCubes = joinableCubes.filter((cube) => !isCubeUsed(cube.name));

  return useMemo(() => {
    if (isQueryEmpty && !selectedCube) {
      return null;
    }

    const isUnconnectable =
      selectedCube &&
      // @ts-ignore
      selectedCube?.type !== 'view' &&
      (isQueryEmpty ||
        (connectedCubes?.length === 1 &&
          // @ts-ignore
          selectedCube?.connectedComponent === connectionId &&
          !isCubeUsed(selectedCube?.name)) ||
        !connectedCubes?.length);

    return !isQueryEmpty || selectedCube ? (
      <Flex flow="column" gap="1x">
        <Space placeContent="space-between">
          <Space gap="1x">
            {actions}
            {/* @ts-ignore */}
            {selectedCube && selectedCube?.type !== 'view' ? (
              <TooltipProvider
                title={
                  isUnconnectable ? 'Show the list of cubes' : 'Connect more cubes to your query'
                }
              >
                <Button
                  aria-label="Return to the list of cubes"
                  size="small"
                  type="primary"
                  icon={isUnconnectable ? <UnorderedListOutlined /> : <PlusOutlined />}
                  onPress={() => selectCube(null)}
                >
                  Cubes
                </Button>
              </TooltipProvider>
            ) : (
              <Title level={3} preset="h5">
                Query
              </Title>
            )}
            {isVerifying && <LoadingOutlined />}
          </Space>
          <Space gap=".5x">
            <TooltipProvider title="Reset the query">
              <Button
                qa="ResetQuery"
                aria-label="Reset the query"
                size="small"
                type="secondary"
                theme="danger"
                icon={<CloseIcon />}
                onPress={() => {
                  clearQuery();
                  selectCube(null);
                  onReset?.();
                }}
              >
                Reset
              </Button>
            </TooltipProvider>
          </Space>
        </Space>
        <Flex flow="column" gap="1ow">
          {Object.entries(queryStats).map(([cubeName, cubeStats]) => {
            return (
              <Flex key={cubeName} flow="column">
                <ListCube
                  rightIcon={!!selectedCube ? null : 'arrow'}
                  name={cubeName}
                  title={cubeStats.instance?.title}
                  // @ts-ignore
                  description={cubeStats?.instance?.description}
                  // @ts-ignore
                  isPrivate={cubeStats?.instance?.public === false}
                  // @ts-ignore
                  type={cubeStats?.instance?.type}
                  stats={selectedCube ? cubeStats : undefined}
                  isSelected={cubeName === selectedCube?.name}
                  isMissing={!cubeStats.instance}
                  onItemSelect={() => selectCube(cubeName)}
                />
                {!selectedCube && (
                  <Block padding=".5x 0 0 2x" margin="0 0 0 2.25x" border="1ow #purple.20 left">
                    <Flex gap="1ow .5x" flow="row wrap">
                      {TYPES.map((type) => {
                        const section = TYPE_PROPS_MAP[type];
                        const count = cubeStats[section]?.length || 0;

                        if (!count) {
                          return null;
                        }

                        return cubeStats[section].map((memberName, i) => {
                          const member =
                            members.measures[memberName] ||
                            members.dimensions[memberName] ||
                            members.segments[memberName];

                          return (
                            <TooltipProvider
                              key={memberName}
                              activeWrap
                              title={
                                !member ? (
                                  <>
                                    Member not found: <b>{memberName}</b>
                                  </>
                                ) : (
                                  <>
                                    {capitalize(type)}: <b>{memberName}</b>
                                    <br />
                                    {/* @ts-ignore */}
                                    {member?.description}
                                  </>
                                )
                              }
                            >
                              <MemberBadge type={member ? type : undefined}>
                                <MemberLabelText preset="tag" data-member={type}>
                                  <span>
                                    <span data-element="MemberName">
                                      {memberName.split('.')[1]}
                                    </span>
                                  </span>
                                </MemberLabelText>
                              </MemberBadge>
                            </TooltipProvider>
                          );
                        });
                      })}
                    </Flex>
                  </Block>
                )}
              </Flex>
            );
          })}
          {selectedCube && !isCubeUsed(selectedCube.name) ? (
            <ListCube
              rightIcon={null}
              // @ts-ignore
              type={selectedCube?.type}
              isSelected={true}
              name={selectedCube?.name}
              title={selectedCube?.title}
              // @ts-ignore
              description={selectedCube?.description}
              // @ts-ignore
              isPrivate={selectedCube?.public === false}
              isMissing={!selectedCube}
            />
          ) : null}
          {selectedCube && (
            <TooltipProvider
              title={`Expand query${
                // @ts-ignore
                selectedCube?.type === 'cube' ? ' and show the list of cubes' : ''
              }`}
              placement="right"
            >
              <Button
                aria-label="Expand query"
                size="small"
                padding="0"
                height="3x"
                width="auto"
                icon={<CaretDownFilled />}
                onPress={() => selectCube(null)}
              />
            </TooltipProvider>
          )}
        </Flex>
      </Flex>
    ) : null;
  }, [
    queryHash,
    isQueryEmpty,
    isVerifying,
    selectedType,
    selectedCube?.name,
    selectedCube && isCubeUsed(selectedCube?.name),
    connectedCubes?.length,
    connectionId,
    cubes,
  ]);
}
