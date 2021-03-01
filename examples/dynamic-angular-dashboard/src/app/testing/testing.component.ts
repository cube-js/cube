import { Component, Inject, OnInit } from '@angular/core';
import { cubejsServiceProvider, CUBEJS_SERVICE } from '../app.module';
import { AuthService } from '../auth.service';
import { CubejsService } from '../cubejs.service';

@Component({
  selector: 'testing',
  template: `
    <div>
      <h1>testig</h1>

      <div>
        <button
          mat-button
          mat-raised-button
          (click)="authService.login('alex', 'test')"
        >
          Log in ({{ cubejsService.token }})
        </button>
        
        <button
          mat-button
          mat-raised-button
          (click)="authService.logout()"
        >
          Logout
        </button>
        
        <div>x: {{ cubejsService.token }}</div>
        
        <p *ngIf="authService.isAuthorized">Hello, username!</p>
        <p *ngIf="!authService.isAuthorized">guest*</p>
      </div>
    </div>
  `,
  // providers: [cubejsServiceProvider],
})
export class TestingComponent implements OnInit {
  constructor(
    // public cubejsService: CubejsService,
    @Inject(CUBEJS_SERVICE) public cubejsService,
    public authService: AuthService
  ) {
    // console.log('construct >>>>>>', Boolean(this.cubejsService));
  }

  ngOnInit() {
    // console.log('.....', this.cubejsService.token);
  }
}
