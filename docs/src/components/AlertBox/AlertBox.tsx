import React from 'react';

export enum AlertBoxTypes {
  DANGER = 'danger',
  INFO = 'info',
  SUCCESS = 'success',
  WARNING = 'warning',
}

export type AlertBoxProps = {
  children: string;
  heading?: string;
  type: AlertBoxTypes;
}

export const AlertBox = ({ children, heading, type }: AlertBoxProps) => {
  const header = heading
    ? <div className="custom-block-heading">{heading}</div>
    : null;

  return (
    <div className={`custom-block ${type || AlertBoxTypes.INFO}`}>
      {header}
      <div className="custom-block-body">
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
