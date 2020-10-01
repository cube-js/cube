import { CdkDragDrop } from '@angular/cdk/drag-drop';
import { Component, Input, OnInit } from '@angular/core';
import { TSourceAxis } from '@cubejs-client/core';
import type { PivotConfig } from '@cubejs-client/ngx';

@Component({
  selector: 'app-pivot',
  templateUrl: './pivot.component.html',
  styleUrls: ['./pivot.component.css'],
})
export class PivotComponent implements OnInit {
  @Input()
  pivotConfig: PivotConfig;
  x = [];
  y = [];

  ngOnInit() {
    this.pivotConfig.subject.subscribe(({ x, y }) => {
      this.x = x;
      this.y = y;
    });
  }

  drop(event: CdkDragDrop<string[]>) {
    this.pivotConfig.moveItem(
      event.previousIndex,
      event.currentIndex,
      event.previousContainer.id as TSourceAxis,
      event.container.id as TSourceAxis
    );
  }
}
