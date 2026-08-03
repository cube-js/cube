import { Action, CubeActionProps, TooltipProvider, tasty, InfoCircleIcon } from '@cube-dev/ui-kit';

export type InfoTooltipButtonProps = {
  tooltipSuffix?: string;
  tooltip: string;
} & CubeActionProps;

const TooltipButton = tasty(Action, {
  styles: {
    display: 'inline-grid',
    placeItems: 'center',
    radius: '1r',
    width: '2.5x',
    height: '2.5x',
    color: {
      '': '#purple-text',
      pressed: '#purple',
    },
    verticalAlign: 'middle',
    preset: 't3',
  },
});

const DEFAULT_TOOLTIP_SUFFIX = 'Click the icon to learn more.';

export function InfoIconButton(props: InfoTooltipButtonProps) {
  const { tooltipSuffix = DEFAULT_TOOLTIP_SUFFIX, tooltip, ...rest } = props;

  return (
    <TooltipProvider
      title={
        <>
          {tooltip} {DEFAULT_TOOLTIP_SUFFIX !== tooltipSuffix || rest.to ? tooltipSuffix : ''}
        </>
      }
      width="initial max-content 40x"
    >
      <TooltipButton {...rest}>
        <InfoCircleIcon />
      </TooltipButton>
    </TooltipProvider>
  );
}
