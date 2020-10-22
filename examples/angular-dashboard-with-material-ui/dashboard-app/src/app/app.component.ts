import { Component } from '@angular/core';



@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent {
  public links = [
    {name: 'Dashboard', href: '/', icon: 'dashboard'},
    {name: 'Orders', href: '/table', icon: 'assignment'}
    ];
  title = 'Angular Material UI Dashboard';
}
