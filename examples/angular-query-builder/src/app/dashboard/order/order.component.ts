import { Component, Input, OnInit } from '@angular/core';
import {
  CdkDragDrop,
  moveItemInArray,
  transferArrayItem,
} from '@angular/cdk/drag-drop';
import type {
  Order,
  TOrder,
  TOrderMember,
} from '@cubejs-client/ngx/dist/src/query-builder/query-members';

@Component({
  selector: 'app-order',
  templateUrl: './order.component.html',
  styleUrls: ['./order.component.css'],
})
export class OrderComponent implements OnInit {
  @Input()
  order: Order;

  ngOnInit(): void {}

  drop(event: CdkDragDrop<string[]>) {
    this.order.reorder(event.previousIndex, event.currentIndex);
    // moveItemInArray(this.orderm, event.previousIndex, event.currentIndex);
    // if (event.previousContainer === event.container) {
    //   moveItemInArray(event.container.data, event.previousIndex, event.currentIndex);
    // } else {
    //   transferArrayItem(event.previousContainer.data, event.container.data, event.previousIndex, event.currentIndex);
    // }
  }

  changeOrder(orderMember: TOrderMember) {
    const orderOptions: TOrder[] = ['asc', 'desc', 'none'];

    function getNextOrder(order: TOrder): TOrder {
      const index = orderOptions.indexOf(order) + 1;
      return orderOptions[index > 2 ? 0 : index];
    }

    this.order.setMemberOrder(orderMember.id, getNextOrder(orderMember.order));
  }
}
