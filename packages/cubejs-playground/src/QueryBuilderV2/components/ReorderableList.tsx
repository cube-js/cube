import { Item as BaseItem, mergeProps, tasty } from '@cube-dev/ui-kit';
import { ReactElement, ReactNode, useRef } from 'react';
import {
  AriaButtonProps,
  DroppableCollectionReorderEvent,
  ListDropTargetDelegate,
  ListKeyboardDelegate,
  useDraggableCollection,
  useDraggableItem,
  useDropIndicator,
  useDroppableCollection,
  useFocusRing,
  useListBox,
  useOption,
} from 'react-aria';
import {
  DraggableCollectionState,
  DroppableCollectionState,
  ListProps,
  ListState,
  useDraggableCollectionState,
  useDroppableCollectionState,
  useListState,
  ItemProps as BaseItemProps,
} from 'react-stately';
import { Key, Node } from '@react-types/shared';

type ItemProps = {
  id: string;
  textValue: string;
  render: (dragButtonProps: AriaButtonProps) => ReactNode;
};

const ReorderableListElement = tasty({
  as: 'ul',
  styles: {
    display: 'contents',
    flow: {
      '': 'row',
      vertical: 'column',
    },
    padding: 0,
    margin: 0,
  },
});

interface ReorderableListProps extends ListProps<ItemProps> {
  direction: 'vertical' | 'horizontal';
  isDisabled?: boolean;
  hasDragButton?: boolean;
  onMove: (newKeys: string[]) => void;
}

export function ReorderableList<T extends ItemProps = ItemProps>(props: ReorderableListProps) {
  let {
    onMove,
    direction = 'horizontal',
    isDisabled,
    hasDragButton,
    children,
    ...itemProps
  } = props;
  let state = useListState(props);
  let ref = useRef(null);
  let { listBoxProps } = useListBox(
    {
      ...itemProps,
      // Prevent dragging from changing selection.
      shouldSelectOnPressUp: true,
    },
    state,
    ref
  );

  const onReorder = (e: DroppableCollectionReorderEvent) => {
    if (isDisabled) {
      return null;
    }

    const originalKeys = [...(itemProps.items || [])].map((item) => item.id);
    const { target, keys: movableKeys } = e;
    const { dropPosition, key: targetKey } = target;
    const movableKey = [...movableKeys][0] as string;

    // Get indices for the movable and target keys
    const movableIndex = originalKeys.indexOf(movableKey);
    const targetIndex = originalKeys.indexOf(targetKey as string);

    // If positions are the same, do nothing
    if (movableIndex === targetIndex) {
      onMove(originalKeys);

      return;
    }

    // Remove the movable key from its original position
    originalKeys.splice(movableIndex, 1);

    // Insert the movable key at the new position
    const insertIndex = dropPosition === 'before' ? targetIndex : targetIndex + 1;
    originalKeys.splice(insertIndex, 0, movableKey);

    onMove(originalKeys);
  };

  // Setup drag state for the collection.
  let dragState = useDraggableCollectionState({
    // Pass through events from props.
    ...itemProps,

    // Collection and selection manager come from list state.
    collection: state.collection,
    selectionManager: state.selectionManager,

    // Provide data for each dragged item. This function could
    // also be provided by the user of the component.
    getItems: (keys: Set<Key>) => {
      if (isDisabled) {
        return [];
      }

      return [...keys].map((key: any) => {
        let item = state.collection.getItem(key);

        return {
          'text/plain': item?.textValue || '',
        };
      });
    },
    getAllowedDropOperations: () => ['move'],
  });

  useDraggableCollection(props, dragState, ref);

  let dropState = useDroppableCollectionState({
    ...itemProps,
    onReorder,
    collection: state.collection,
    selectionManager: state.selectionManager,
  });

  let { collectionProps } = useDroppableCollection(
    {
      ...itemProps,
      // Provide drop targets for keyboard and pointer-based drag and drop.
      keyboardDelegate: new ListKeyboardDelegate(state.collection, state.disabledKeys, ref),
      dropTargetDelegate: new ListDropTargetDelegate(state.collection, ref),
      onReorder,
    },
    dropState,
    ref
  );

  return (
    <ReorderableListElement
      {...mergeProps(listBoxProps, collectionProps, {
        mods: { vertical: direction === 'vertical' },
      })}
      ref={ref}
    >
      {[...state.collection].map((item) => {
        return (
          <ReorderableItem
            key={item.key}
            item={item}
            direction={direction}
            state={state}
            isDisabled={isDisabled}
            hasDragButton={hasDragButton}
            dragState={dragState}
            dropState={dropState}
          />
        );
      })}
    </ReorderableListElement>
  );
}

const ReorderableMemberElement = tasty({
  as: 'div',
  styles: {
    position: 'sticky',
    top: 0,
    display: 'block',
    shadow: {
      '': '0',
      focused: '1bw solid #focus inset',
    },
  },
});

interface ReorderableItemProps {
  item: Node<ItemProps>;
  state: ListState<any>;
  direction: 'vertical' | 'horizontal';
  hasDragButton?: boolean;
  isDisabled?: boolean;
  dragState: DraggableCollectionState;
  dropState: DroppableCollectionState;
}

function ReorderableItem({
  item,
  state,
  direction = 'horizontal',
  hasDragButton,
  isDisabled,
  dragState,
  dropState,
}: ReorderableItemProps) {
  // Set up the listbox option as normal. See useListBox docs for details.
  let ref = useRef(null);
  let { optionProps } = useOption({ key: item.key }, state, ref);
  let { isFocusVisible, focusProps } = useFocusRing();

  // Register the item as a drag source.
  let { dragProps, dragButtonProps } = useDraggableItem(
    {
      key: item.key,
      hasDragButton,
      hasAction: true,
    },
    dragState
  );

  return (
    <>
      <ReorderableMemberElement
        {...mergeProps(optionProps, !isDisabled ? dragProps : {}, focusProps)}
        ref={ref}
        mods={{
          focused: isFocusVisible,
        }}
      >
        <DropIndicator
          direction={direction}
          position="before"
          target={{ type: 'item', key: item.key, dropPosition: 'before' }}
          dropState={dropState}
        />
        {hasDragButton ? item.value?.render(dragButtonProps) : item.rendered}
        {state.collection.getKeyAfter(item.key) == null && (
          <DropIndicator
            direction={direction}
            position="after"
            target={{ type: 'item', key: item.key, dropPosition: 'after' }}
            dropState={dropState}
          />
        )}
      </ReorderableMemberElement>
    </>
  );
}

const DropIndicatorElement = tasty({
  styles: {
    zIndex: 10,
    position: 'absolute',
    pointerEvents: 'none',
    opacity: {
      '': 0,
      dropTarget: 1,
    },
    fill: '#purple',
    width: {
      '': '.5x',
      vertical: '100%',
    },
    height: {
      '': '.5x',
      horizontal: '100%',
    },
    top: {
      '': 'auto',
      'vertical & before': '-2px',
    },
    bottom: {
      '': 'auto',
      'vertical & after': '-2px',
    },
    left: {
      '': 'auto',
      'horizontal & before': '-2px',
    },
    right: {
      '': 'auto',
      'horizontal & after': '-2px',
    },
  },
});

interface DropIndicatorProps {
  position: 'before' | 'after';
  target: any;
  dropState: any;
  direction: 'vertical' | 'horizontal';
}

function DropIndicator(props: DropIndicatorProps) {
  const { position, direction = 'horizontal', target } = props;

  let ref = useRef(null);
  let { dropIndicatorProps, isHidden, isDropTarget } = useDropIndicator(
    { target },
    props.dropState,
    ref
  );
  if (isHidden) {
    return null;
  }

  return (
    <DropIndicatorElement
      ref={ref}
      role="option"
      {...dropIndicatorProps}
      mods={{
        'drop-target': isDropTarget,
        after: position === 'after',
        before: position === 'before',
        vertical: direction === 'vertical',
        horizontal: direction !== 'vertical',
      }}
    />
  );
}

type ItemComponent = <T>(props: BaseItemProps<T> & ItemProps) => ReactElement;

ReorderableList.Item = Object.assign(BaseItem, {
  displayName: 'Item',
}) as ItemComponent;
