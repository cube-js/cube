import { Badge, TooltipProvider } from '@cube-dev/ui-kit';

import { useQueryBuilderContext } from '../context';

export function OutdatedLabel() {
  let { isApiTokenChanged, isDataModelChanged, isQueryTouched, isResultOutdated } =
    useQueryBuilderContext();

  let title = (
    <>
      {isApiTokenChanged && <div>Security context has changed</div>}
      {isDataModelChanged && <div>Data model has been updated</div>}
      {isQueryTouched && <div>Query has changed</div>}
    </>
  );

  return (
    <TooltipProvider activeWrap title={title}>
      <Badge type="disabled" styles={{ cursor: 'default' }}>
        OUTDATED
      </Badge>
    </TooltipProvider>
  );
}
