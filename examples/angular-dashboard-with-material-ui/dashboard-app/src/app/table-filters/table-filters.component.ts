import { Component, EventEmitter, OnInit, Output } from "@angular/core";
import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';

@Component({
  selector: 'app-table-filters',
  templateUrl: './table-filters.component.html',
  styleUrls: ['./table-filters.component.scss']
})
export class TableFiltersComponent implements OnInit {
  @Output() statusChanged = new EventEmitter();
  @Output() dateChange = new EventEmitter();
  @Output() sliderChanged = new EventEmitter();

  cols = 4;

  statusChangedFunc = (obj) => {
    this.statusChanged.emit(obj.value);
  };
  changeDate(number, date) {
    this.dateChange.emit({number, date});
  };
  formatLabel(value: number) {
    if (value >= 1000) {
      return '$' + Math.round(value / 1000) + 'k';
    } else {
      return '$' + value
    }
    return value;
  }
  sliderChange(value) {
    this.sliderChanged.emit(value);
  }

  constructor(private breakpointObserver: BreakpointObserver) {
    this.breakpointObserver.observe([
      Breakpoints.XSmall,
      Breakpoints.Small,
      Breakpoints.Medium,
      Breakpoints.Large,
      Breakpoints.XLarge,
    ]).subscribe(result => {
      if (result.matches) {
        if (result.breakpoints[Breakpoints.XSmall]) {
          this.cols = 1;
        }
        if (result.breakpoints[Breakpoints.Small]) {
          this.cols = 2;
        }
        if (result.breakpoints[Breakpoints.Medium]) {
          this.cols = 4;
        }
        if (result.breakpoints[Breakpoints.Large]) {
          this.cols = 4;
        }
        if (result.breakpoints[Breakpoints.XLarge]) {
          this.cols = 4;
        }
      }
    });
  }

  ngOnInit(): void {
  }

}
