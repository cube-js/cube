import { Component } from '@angular/core';
import { Builder } from './builder/builder.service';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css'],
  providers: [Builder],
})
export class AppComponent {
  constructor(private builder: Builder) {
    // console.log('AppComponent');
    // builder.state.subscribe((vizState) => console.log(JSON.stringify(vizState)));

    // builder.order.reorder(1, 1);
    // // setTimeout(() => builder.pivotConfig.move(1, 2), 3000);
    
    // builder.deserializeState({
    //   order: [['yyy', 'djjj']]
    // })
  }
}
