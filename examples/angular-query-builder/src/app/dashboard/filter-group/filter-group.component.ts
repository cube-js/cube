import {
  Component,
  EventEmitter,
  Input,
  OnDestroy,
  OnInit,
  Output,
} from '@angular/core';
import { FilterMember } from '@cubejs-client/ngx';
import { BehaviorSubject } from 'rxjs';

@Component({
  selector: 'filter-group',
  templateUrl: './filter-group.component.html',
})
export class FilterGroupComponent implements OnInit, OnDestroy {
  currentFilter = new BehaviorSubject<any>(null);

  @Input()
  filters: FilterMember;

  @Input()
  members: any[];

  @Input()
  allMembers: any[];

  ngOnInit(): void {
    this.currentFilter.subscribe((filter) => {
      if (filter?.operator && filter?.values.length) {
        this.filters.add({
          dimension: filter.name,
          operator: filter.operator,
          values: filter.values,
        });

        this.currentFilter.next(null);
      }
    });
  }

  ngOnDestroy() {
    this.currentFilter.unsubscribe();
  }

  onMemberSelect(event: any) {
    this.currentFilter.next({
      ...this.allMembers.find(({ name }) => event.value === name),
      values: [],
    });
  }

  handleOperatorChange(operator: any) {
    this.currentFilter.next({
      ...this.currentFilter.value,
      operator,
    });
  }

  handleValueChange(value: any) {
    this.currentFilter.next({
      ...this.currentFilter.value,
      values: [value],
    });
  }
}

@Component({
  selector: 'filter',
  templateUrl: './filter.component.html',
  styleUrls: ['./filter-group.component.css'],
})
export class FilterComponent {
  @Input()
  member;

  @Output()
  onOperatorSelect = new EventEmitter<string>();

  @Output()
  onValueChange = new EventEmitter<string>();

  @Output()
  onRemove = new EventEmitter<string>();
}
