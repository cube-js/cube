import type { ComponentProps } from 'react';

export const H1 = (props: ComponentProps<'h1'>) => {
  // console.log({ props });
  return (
    <h1
      className="nx-mt-2 nx-text-4xl nx-font-bold nx-tracking-tight"
      {...props}
    />
  );
}
