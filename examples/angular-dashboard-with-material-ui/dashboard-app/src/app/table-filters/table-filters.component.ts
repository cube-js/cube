import { Component, EventEmitter, OnInit, Output } from "@angular/core";

@Component({
  selector: 'app-table-filters',
  templateUrl: './table-filters.component.html',
  styleUrls: ['./table-filters.component.scss']
})
export class TableFiltersComponent implements OnInit {
  @Output() statusChanged = new EventEmitter();
  statusChangedFunc = (obj) => {
    this.statusChanged.emit(obj.value);
  };

  constructor() { }

  ngOnInit(): void {
  }

}
