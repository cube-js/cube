import {
  Button,
  CaretDownIcon,
  ComboBox,
  Grid,
  InfoCircleIcon,
  NumberInput,
  Space,
  Tag,
  tasty,
  TextInput,
  TooltipProvider,
} from '@cube-dev/ui-kit';
import { Key } from '@react-types/shared';
import React, { KeyboardEvent, useCallback, useEffect, useRef, useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';

import { useQueryBuilderContext } from '../context';
import { useEvent, useOutsideFocus, useDimensionValues } from '../hooks';

const ButtonWrapper = tasty({
  styles: {
    width: '4x',
    height: '4x',
    display: 'grid',
    radius: true,
    border: true,
    placeItems: 'baseline',
  },
});

const AddButton = tasty(Button, {
  size: 'small',
  icon: <PlusOutlined />,
  styles: {
    width: '(4x - 2bw)',
    height: '(4x - 2bw)',
    radius: {
      '': true,
      inside: 'right',
    },
  },
});

const StyledTag = tasty(Tag, {
  isClosable: true,
  styles: {
    placeItems: 'baseline',
    preset: 't3',
    padding: '.625x 2.75x .625x .75x',
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    width: 'max 100%',
  },
  closeButtonStyles: {
    placeSelf: 'stretch',
    fill: {
      '': '#clear',
      ':hover': '#dark.05',
    },
    preset: 't3',
    padding: '0 .25x',
  },
});

interface ValuesInputProps {
  type?: 'string' | 'number';
  memberName?: string;
  memberType?: 'measure' | 'dimension';
  placeholder?: string;
  isCompact?: boolean;
  allowSuggestions?: boolean;
  values: string[];
  onChange: (values: string[]) => void;
}

export function ValuesInput(props: ValuesInputProps) {
  const {
    type = 'string',
    memberName,
    memberType,
    allowSuggestions,
    placeholder,
    values,
    isCompact,
    onChange,
  } = props;

  const [isOpen, setIsOpen] = useState(!values.length);
  const [hasError, setHasError] = useState(false);
  const [textValue, setTextValue] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);

  const ref = useRef<HTMLElement>();
  const inputRef = useRef<HTMLInputElement>(null);

  const { cubeApi, mutexObj } = useQueryBuilderContext();

  const {
    suggestions,
    isLoading: isSuggestionLoading,
    error: suggestionError,
  } = useDimensionValues({
    cubeApi,
    mutexObj,
    dimension: memberName,
    skip:
      !isOpen || !allowSuggestions || !showSuggestions || !memberName || memberType !== 'dimension',
  });

  // If focus goes outside the widget, update the state
  useOutsideFocus(
    ref,
    useEvent(() => {
      if (textValue && textValue.trim()) {
        addValueLazy();
      }
    })
  );

  const onAddButtonPress = () => {
    if (isOpen) {
      addValue();
    } else {
      setIsOpen(true);
    }
  };

  const onFocus = () => {
    setHasError(false);
  };

  function focusOnInput() {
    setTimeout(() => {
      ref.current?.querySelector('input')?.focus();
    }, 100);
  }

  // If input is shown, then focus on it
  useEffect(() => {
    if (isOpen && ref.current) {
      focusOnInput();
    }
  }, [isOpen]);

  useEffect(() => {
    if (!isSuggestionLoading) {
      focusOnInput();
    }
  }, [isSuggestionLoading]);

  // Add current value to the value list and clear the input value
  const addValue = useEvent(() => {
    const value = textValue.trim();

    if (!value) {
      return;
    }

    onChange([...values.filter((val) => val !== value), value]);
    setTextValue('');
    setIsOpen(false);
  });

  const addValueLazy = () => {
    setTimeout(() => {
      addValue();
    });
  };

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      addValueLazy();
    }
    if (e.key === 'Escape') {
      setTextValue('');
      setIsOpen(false);
    }
  };

  const onRemove = useCallback(
    (value) => {
      const newValues = values.filter((val) => val !== value);

      if (!newValues.length) {
        setHasError(true);
        setIsOpen(true);
      }

      onChange(newValues);
    },
    [values.length]
  );

  const onTextChange = useEvent((value: string | number) => {
    setTextValue(typeof value === 'number' ? (!Number.isNaN(value) ? String(value) : '') : value);
  });

  function onInput() {
    if (inputRef?.current) {
      if (inputRef.current.value && inputRef.current.value.trim()) {
        const value = String(inputRef.current.value.replaceAll(',', ''));

        if (!Number.isNaN(value)) {
          setTextValue(String(value));
        }
      } else {
        setTextValue('');
      }
    }
  }

  const addButton = (
    <AddButton
      isDisabled={!textValue.length && isOpen}
      mods={{ inside: isOpen }}
      onPress={onAddButtonPress}
    />
  );

  const input =
    type === 'string' ? (
      memberType === 'dimension' && allowSuggestions && showSuggestions ? (
        <ComboBox
          allowsCustomValue
          aria-label="Text value input"
          inputRef={inputRef}
          size="small"
          inputValue={textValue}
          placeholder={
            isSuggestionLoading
              ? 'Loading values...'
              : (placeholder ?? `Type ${suggestions.length ? 'or select ' : ''}value to add...`)
          }
          validationState={hasError ? 'invalid' : undefined}
          suffix={
            suggestionError ? (
              <TooltipProvider activeWrap title={`Unable to load values.\n${suggestionError}`}>
                <InfoCircleIcon color="#danger" styles={{ cursor: 'default' }} />
              </TooltipProvider>
            ) : null
          }
          suffixPosition="after"
          width="30x"
          menuTrigger="focus"
          isLoading={isSuggestionLoading && !suggestions.length}
          disabledKeys={suggestions.length ? undefined : ['no-suggestions']}
          onSelectionChange={(key: Key | null) => {
            key && onTextChange(key as string);
            addValueLazy();
          }}
          onInputChange={(key: Key | null) => {
            onTextChange(key as string);
          }}
          onKeyDown={onKeyDown}
          onFocus={onFocus}
        >
          {suggestions.length ? (
            suggestions.map((suggestion) => (
              <ComboBox.Item key={suggestion} textValue={suggestion}>
                {suggestion}
              </ComboBox.Item>
            ))
          ) : (
            <ComboBox.Item key="no-suggestions">No values loaded</ComboBox.Item>
          )}
        </ComboBox>
      ) : (
        <TextInput
          aria-label="Text value input"
          inputRef={inputRef}
          size="small"
          value={textValue}
          placeholder={placeholder || `Type ${allowSuggestions ? 'or select ' : ''}value to add...`}
          validationState={hasError ? 'invalid' : undefined}
          isLoading={isSuggestionLoading}
          suffix={
            allowSuggestions && !suggestionError ? (
              !isSuggestionLoading ? (
                <TooltipProvider title="Load values...">
                  <Button
                    icon={<CaretDownIcon />}
                    type="neutral"
                    size="small"
                    height="(4x - 2bw)"
                    radius="right"
                    onPress={() => setShowSuggestions(true)}
                  />
                </TooltipProvider>
              ) : null
            ) : suggestionError && !hasError ? (
              <Grid width="4x" placeContent="center">
                <TooltipProvider activeWrap title={`Unable to load values.\n${suggestionError}`}>
                  <InfoCircleIcon color="#danger" styles={{ cursor: 'default' }} />
                </TooltipProvider>
              </Grid>
            ) : null
          }
          suffixPosition="after"
          wrapperStyles={{ width: '30x' }}
          onChange={onTextChange}
          onKeyDown={onKeyDown}
          onFocus={onFocus}
        />
      )
    ) : (
      <NumberInput
        aria-label="Number value input"
        inputRef={inputRef}
        size="small"
        value={parseFloat(textValue)}
        placeholder={placeholder || 'Type value to add...'}
        suffix={addButton}
        validationState={hasError ? 'invalid' : undefined}
        suffixPosition="after"
        wrapperStyles={{ width: '20x' }}
        onInput={onInput}
        onChange={onTextChange}
        onKeyDown={onKeyDown}
        onFocus={onFocus}
      />
    );

  const Element = useCallback(
    ({ children }: React.PropsWithChildren<{}>) => {
      if (isCompact) {
        return <>{children}</>;
      }

      return (
        <Space ref={ref} flow="row wrap" placeContent="baseline" gap="1x">
          {children}
        </Space>
      );
    },
    [isCompact]
  );

  return (
    <Element>
      {values.map((value, i) => {
        return (
          <TooltipProvider key={i} activeWrap title={value}>
            <StyledTag onClose={() => onRemove(value)}>{value}</StyledTag>
          </TooltipProvider>
        );
      })}
      <Space gap={0} placeContent="baseline">
        {isOpen ? input : <ButtonWrapper>{addButton}</ButtonWrapper>}
      </Space>
    </Element>
  );
}
