import { Component, OnInit, Input } from "@angular/core";
import { CubejsClient } from "@cubejs-client/ngx";

@Component({
  selector: "app-material-table",
  templateUrl: "./material-table.component.html",
  styleUrls: ["./material-table.component.scss"]
})
export class MaterialTableComponent implements OnInit {
  @Input() query: object;

  constructor(private cubejs: CubejsClient) {
  }
  public dataSource = [];
  displayedColumns = ['id', 'size', 'name', 'city', 'price', 'status', 'date'];

  ngOnInit(): void {
    this.cubejs.load(this.query).subscribe(
      resultSet => {
        this.dataSource = resultSet.tablePivot();
      },
      err => console.log("HTTP Error", err)
    );
  }

}
