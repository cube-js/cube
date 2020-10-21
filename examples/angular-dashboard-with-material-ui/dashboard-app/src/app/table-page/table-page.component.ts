import { Component, OnInit } from '@angular/core';
import { BehaviorSubject } from "rxjs";

@Component({
  selector: 'app-table-page',
  templateUrl: './table-page.component.html',
  styleUrls: ['./table-page.component.scss']
})
export class TablePageComponent implements OnInit {
  public _query = new BehaviorSubject({
    "limit": 12,
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "day"
      }
    ],
    "dimensions": [
      "Users.id",
      "Orders.id",
      "Orders.size",
      "Users.fullName",
      "Users.city",
      "Orders.price",
      "Orders.status",
      "Orders.createdAt"
    ]
  });
  public query = {};

  constructor() { }

  ngOnInit(): void {
    this._query.subscribe(query => {
      this.query = query;
    });
  }

}
