import React from 'react';
import classes from './AlertBox.module.css';
import classnames from 'classnames/bind';
const cn = classnames.bind(classes);

export enum AlertBoxTypes {
  DANGER = 'danger',
  INFO = 'info',
  SUCCESS = 'success',
  WARNING = 'warning',
}

declare const TypeToEmoji: {
  default: string;
  error: string;
  info: JSX.Element;
  warning: string;
};
type CalloutType = keyof typeof TypeToEmoji;

export type AlertBoxProps = {
  children: string;
  heading?: string;
  type: AlertBoxTypes;
}

const typeMapping: Record<AlertBoxTypes, CalloutType> = {
  'danger': 'error',
  info: 'info',
  warning: 'warning',
  success: 'default',
}

const iconMapping: Record<string, any> = {
  'danger': '🚫',
  info: 'ℹ️',
  warning: '⚠️',
  success: '✅',
};

export const AlertBox = ({ children, heading, type }: AlertBoxProps) => {
  const header = heading
    ? (
      <div className={classes.AlertBox__header}>
        <span className={cn('AlertBox__HeaderIcon')}>{iconMapping[type]}</span>
        {heading}
      </div>
    )
    : null;

  return (
    <div className={cn('AlertBox__Wrapper', `AlertBox__Wrapper--${typeMapping[type]}`)}>
      {header}
      <div className={classes.AlertBox__content}>
        {children}
      </div>
    </div>
  )
}

export type AlertBoxSubclass = Omit<AlertBoxProps, 'type'>;

export type DangerBoxProps = AlertBoxSubclass;
export const DangerBox = (props: DangerBoxProps) => <AlertBox type={AlertBoxTypes.DANGER} {...props} />;

export type InfoBoxProps = AlertBoxSubclass;
export const InfoBox = (props: InfoBoxProps) => <AlertBox type={AlertBoxTypes.INFO} {...props} />;

export type SuccessBoxProps = AlertBoxSubclass;
export const SuccessBox = (props: SuccessBoxProps) => <AlertBox type={AlertBoxTypes.SUCCESS} {...props} />;

export type WarningBoxProps = AlertBoxSubclass;
export const WarningBox = (props: WarningBoxProps) => <AlertBox type={AlertBoxTypes.WARNING} {...props} />;
