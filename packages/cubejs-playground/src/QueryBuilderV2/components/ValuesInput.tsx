import {
  Button,
  NumberInput,
  Space,
  Tag,
  tasty,
  TextInput,
  TooltipProvider,
} from '@cube-dev/ui-kit';
import React, { KeyboardEvent, useCallback, useEffect, useRef, useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';

import { useOutsideFocus } from '../hooks';

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
    radius: '(1r - 1bw) right',
    width: '(4x - 2bw)',
    height: '(4x - 2bw)',
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
  isCompact?: boolean;
  values: string[];
  onChange: (values: string[]) => void;
}

export function ValuesInput(props: ValuesInputProps) {
  const { type = 'string', values, isCompact, onChange } = props;

  const [open, setOpen] = useState(!values.length);
  const [error, setError] = useState(!values.length);
  const [textValue, setTextValue] = useState('');

  const ref = useRef<HTMLElement>();
  const inputRef = useRef<HTMLInputElement | HTMLTextAreaElement>(null);

  // If focus goes outside the widget, then clear the value and hid the input
  useOutsideFocus(
    ref,
    useCallback(() => {
      setTextValue('');
      if (values.length) {
        setOpen(false);
      } else {
        setError(true);
      }
    }, [values.length, ref?.current])
  );

  const onAddButtonPress = () => {
    if (open) {
      addValue();
    } else {
      setOpen(true);
    }
  };

  const onFocus = () => {
    setError(false);
  };

  // If input is shown, then focus on it
  useEffect(() => {
    if (open && ref.current) {
      ref.current?.querySelector('input')?.focus();
    }
  }, [open]);

  // Add current value to the value list and clear the input value
  const addValue = () => {
    const value = textValue.trim();

    if (!value) {
      return;
    }

    onChange([...values.filter((val) => val !== value), value]);
    setTextValue('');
  };

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter') {
      addValue();
    }
  };

  const onRemove = useCallback(
    (value) => {
      const newValues = values.filter((val) => val !== value);

      if (!newValues.length) {
        setError(true);
        setOpen(true);
      }

      onChange(newValues);
    },
    [values.length]
  );

  function onTextChange(value: string | number) {
    if (!Number.isNaN(value)) {
      setTextValue(typeof value === 'number' ? String(value) : value);
    }
  }

  function onBlur() {
    if (inputRef?.current) {
      if (!inputRef.current.value || !inputRef.current.value.trim()) {
        inputRef.current.value = textValue;
        setOpen(false);
      }
    }
  }

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

  const addButton = <AddButton isDisabled={!textValue.length && open} onPress={onAddButtonPress} />;

  const input =
    type === 'string' ? (
      <TextInput
        aria-label="Text input"
        inputRef={inputRef}
        size="small"
        value={textValue}
        placeholder="Type value to add..."
        suffix={addButton}
        validationState={error ? 'invalid' : undefined}
        suffixPosition="after"
        onChange={onTextChange}
        onKeyDown={onKeyDown}
        onFocus={onFocus}
      />
    ) : (
      <NumberInput
        aria-label="Number input"
        inputRef={inputRef}
        size="small"
        value={parseFloat(textValue)}
        placeholder="Type value to add..."
        suffix={addButton}
        validationState={error ? 'invalid' : undefined}
        suffixPosition="after"
        wrapperStyles={{ width: '20x' }}
        onBlur={onBlur}
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
        {open ? input : <ButtonWrapper>{addButton}</ButtonWrapper>}
      </Space>
    </Element>
  );
}
