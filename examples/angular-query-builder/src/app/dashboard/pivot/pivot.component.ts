import { CdkDragDrop } from '@angular/cdk/drag-drop';
import { Component, Input } from '@angular/core';
import { TSourceAxis } from '@cubejs-client/core';
import type { PivotConfig } from '@cubejs-client/ngx/dist/src/query-builder/pivot-config';

@Component({
  selector: 'app-pivot',
  templateUrl: './pivot.component.html',
  styleUrls: ['./pivot.component.css'],
})
export class PivotComponent {
  @Input()
  pivotConfig: PivotConfig;

  drop(event: CdkDragDrop<string[]>) {
    this.pivotConfig.moveItem(
      event.previousIndex,
      event.currentIndex,
      event.previousContainer.id as TSourceAxis,
      event.container.id as TSourceAxis
    );
  }
}
