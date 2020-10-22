import { Component, EventEmitter, OnInit, Output } from "@angular/core";

@Component({
  selector: 'app-table-filters',
  templateUrl: './table-filters.component.html',
  styleUrls: ['./table-filters.component.scss']
})
export class TableFiltersComponent implements OnInit {
  @Output() statusChanged = new EventEmitter();
  @Output() dateChange = new EventEmitter();
  @Output() sliderChanged = new EventEmitter();


  statusChangedFunc = (obj) => {
    this.statusChanged.emit(obj.value);
  };
  changeDate(number, date) {
    this.dateChange.emit({number, date});
  };
  formatLabel(value: number) {
    if (value >= 1000) {
      return Math.round(value / 1000) + 'k';
    }
    return value;
  }
  sliderChange(value) {
    this.sliderChanged.emit(value);
  }

  constructor() { }

  ngOnInit(): void {
  }

}
