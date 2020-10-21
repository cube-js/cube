import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-table-filters',
  templateUrl: './table-filters.component.html',
  styleUrls: ['./table-filters.component.scss']
})
export class TableFiltersComponent implements OnInit {
  statusChanged = (obj) => {
    console.log(obj.value);
  };

  constructor() { }

  ngOnInit(): void {
  }

}
