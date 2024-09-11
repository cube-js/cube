import { memo } from 'react';

export const CloseIcon = memo(({ color }: { color?: string }) => {
  return (
    <div
      style={{
        display: 'grid',
        width: '16px',
        height: '16px',
        color: color ? `var(--${color}-color)` : undefined,
      }}
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="16"
        height="16"
        fill="none"
      >
        <path
          fill="currentColor"
          fillRule="evenodd"
          clipRule="evenodd"
          d="M2.626 2.732a.6.6 0 0 0 0 .849L7.046 8l-4.42 4.42a.6.6 0 0 0 0 .848l.141.141a.6.6 0 0 0 .849 0l4.42-4.42 4.348 4.35a.6.6 0 0 0 .849 0l.141-.142a.6.6 0 0 0 0-.848L9.025 8l4.349-4.349a.6.6 0 0 0 0-.848l-.141-.142a.6.6 0 0 0-.849 0L8.035 7.01 3.616 2.59a.6.6 0 0 0-.849 0l-.141.142Z"
        />
      </svg>
    </div>
  );
});
