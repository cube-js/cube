import { memo } from 'react';
import { TooltipProvider } from '@cube-dev/ui-kit';

export const PrimaryKeyIcon = memo(({ color }: { color?: string }) => {
  return (
    <TooltipProvider activeWrap title="This member is a primary key of this cube/view" delay={1000}>
      <svg
        width="16"
        height="16"
        viewBox="0 0 16 16"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        style={{ color: color ? `var(--${color}-color)` : 'var(--dark-02-color)' }}
      >
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M12.95 7.96487C13.262 7.65287 13.5023 7.29631 13.6708 6.91567C14.2354 5.6406 13.9951 4.09539 12.95 3.05026C11.5929 1.69313 9.39251 1.69313 8.03538 3.05026C6.832 4.25364 6.69567 6.12001 7.62639 7.47361L2.99297 12.107C2.74445 12.3555 2.74445 12.7585 2.99297 13.007C3.24148 13.2555 3.6444 13.2555 3.89292 13.007L4.1423 12.7576L4.94504 13.5603C5.23793 13.8532 5.7128 13.8532 6.0057 13.5603L7.26697 12.2991C7.56905 11.997 7.55816 11.504 7.24304 11.2155L6.42928 10.4706L8.52628 8.37362C8.7044 8.49612 8.89139 8.60014 9.08459 8.68569C10.3597 9.25028 11.9049 9.01 12.95 7.96487ZM8.93533 7.06492C9.79544 7.92502 11.1899 7.92502 12.05 7.06492C12.9101 6.20482 12.9101 4.81032 12.05 3.95021C11.1899 3.09011 9.79544 3.09011 8.93534 3.95021C8.07523 4.81032 8.07523 6.20482 8.93533 7.06492Z"
          fill="currentColor"
        />
      </svg>
    </TooltipProvider>
  );
});
