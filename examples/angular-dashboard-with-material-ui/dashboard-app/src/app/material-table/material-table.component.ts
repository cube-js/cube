import { Component, Input, Output } from "@angular/core";
import { CubejsClient } from "@cubejs-client/ngx";
import { EventEmitter } from '@angular/core';

@Component({
  selector: "app-material-table",
  templateUrl: "./material-table.component.html",
  styleUrls: ["./material-table.component.scss"]
})
export class MaterialTableComponent {
  constructor(private cubejs: CubejsClient) {}
  @Input() set query(query: object) {
    this.loading = true;
    this.cubejs.load(query).subscribe(
      resultSet => {
        this.dataSource = resultSet.tablePivot();
        this.loading = false;
      },
      err => console.log("HTTP Error", err)
    );
    this.cubejs.load({...query, limit: 50000, offset: 0}).subscribe(
      resultSet => {
        this.length = resultSet.tablePivot().length;
      },
      err => console.log("HTTP Error", err)
    );
  };
  @Input() limit: number;
  @Output() pageEvent = new EventEmitter();
  @Output() sortingChanged = new EventEmitter();
  loading = true;
  length = 0;
  pageSize = 10;
  pageSizeOptions: number[] = [5, 10, 25, 100];
  dataSource = [];
  displayedColumns = ['id', 'size', 'name', 'city', 'price', 'status', 'date'];
  changeSorting(value) {
    this.sortingChanged.emit(value)
  }
}
