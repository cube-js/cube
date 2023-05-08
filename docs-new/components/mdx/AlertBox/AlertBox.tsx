import React from 'react';
import { Callout } from "nextra-theme-docs";

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

export const AlertBox = ({ children, heading, type }: AlertBoxProps) => {
  const header = heading
    ? <div className="custom-block-heading">{heading}</div>
    : null;

  return (
    <Callout type={typeMapping[type]}>
      {header}
      <div className="custom-block-body">
        {children}
      </div>
    </Callout>
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
