import { Component, Input, Output, EventEmitter } from '@angular/core';
import { MatSelectChange } from '@angular/material/select';
import { TimeDimensionMember } from '@cubejs-client/ngx';

@Component({
  selector: 'time-group',
  templateUrl: './time-group.component.html',
})
export class TimeGroupComponent {
  granularities = [
    { value: '', title: 'w/o grouping' },
    { value: 'hour', title: 'Hour' },
    { value: 'day', title: 'Day' },
    { value: 'week', title: 'Week' },
    { value: 'month', title: 'Month' },
    { value: 'year', title: 'Year' },
  ];
  dateRanges = [
    { title: 'All time' },
    { title: 'Today' },
    { title: 'Yesterday' },
    { title: 'This week' },
    { title: 'This month' },
    { title: 'This quarter' },
    { title: 'This year' },
    { title: 'Last 7 days' },
    { title: 'Last 30 days' },
    { title: 'Last week' },
    { title: 'Last month' },
    { title: 'Last quarter' },
    { title: 'Last year' },
  ];

  @Input()
  timeDimensionMember: TimeDimensionMember;

  @Input()
  members: any;

  @Input()
  allMembers: any[];

  @Output()
  onDateRangeSelect: EventEmitter<string> = new EventEmitter();

  @Output()
  onGranularitySelect: EventEmitter<string> = new EventEmitter();

  get dateRange() {
    return this.timeDimensionMember.asArray()[0]?.dateRange;
  }

  get granularity() {
    return this.timeDimensionMember.asArray()[0]?.granularity;
  }

  handleTimeDimensionSelect(value: string) {
    this.timeDimensionMember.add(value);
  }

  handleDateRangeSelect(event: MatSelectChange) {
    this.timeDimensionMember.setDateRange(
      0,
      event.value === 'All time' ? undefined : event.value
    );
  }

  handleGranularitySelect(event: MatSelectChange) {
    this.timeDimensionMember.setGranularity(0, event.value || undefined);
  }
}
