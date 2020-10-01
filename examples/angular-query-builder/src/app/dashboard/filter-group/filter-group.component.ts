import {
  Component,
  EventEmitter,
  Input,
  OnDestroy,
  OnInit,
  Output,
} from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { MatSelect, MatSelectChange } from '@angular/material/select';
import { FilterMember } from '@cubejs-client/ngx';
import { BehaviorSubject } from 'rxjs';
import { debounceTime } from 'rxjs/operators';

@Component({
  selector: 'filter-group',
  templateUrl: './filter-group.component.html',
  styleUrls: ['./filter-group.component.css'],
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
      if (filter?.operator) {
        this.filters.add({
          member: filter.name,
          operator: filter.operator,
          values: filter.values || [''],
        });

        this.currentFilter.next(null);
      }
    });
  }

  ngOnDestroy() {
    this.currentFilter.unsubscribe();
  }

  selectMember(event: MatSelect) {
    this.currentFilter.next({
      ...this.allMembers.find(({ name }) => event.value === name),
      values: [],
    });
  }

  handleOperatorChange(operator: MatSelect) {
    this.currentFilter.next({
      ...this.currentFilter.value,
      operator,
    });
  }

  handleValueChange(value: string) {
    this.currentFilter.next({
      ...this.currentFilter.value,
      values: [value],
    });
  }

  trackByMethod(_, member: any) {
    return member?.name;
  }
}

@Component({
  selector: 'filter',
  templateUrl: './filter.component.html',
  styleUrls: ['./filter-group.component.css'],
})
export class FilterComponent implements OnInit {
  private _member: any;

  @Input()
  set member(member) {
    const nextValue = member?.values[0] || '';
    if (nextValue !== this.filterValue.value) {
      this.filterValue.setValue(nextValue);
    }
    this._member = member;
  }

  get member() {
    return this._member;
  }

  @Input()
  allMembers: any[];

  @Output()
  onOperatorSelect = new EventEmitter<string>();

  @Output()
  valueChanges = new EventEmitter<string>();

  @Output()
  onRemove = new EventEmitter<string>();

  @Output()
  selectMember = new EventEmitter<MatSelectChange>();

  filterValue = new FormControl('', [Validators.required]);

  ngOnInit() {
    this.filterValue.valueChanges.pipe(debounceTime(300)).subscribe((value) => {
      this.valueChanges.emit(value);
    });
  }
}
