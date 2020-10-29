import { Component, Inject } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { Apollo, gql } from 'apollo-angular';

@Component({
  selector: 'add-to-dashboard-dialog',
  templateUrl: './add-to-dashboard-dialog.component.html',
  styleUrls: ['./add-to-dashboard-dialog.component.css'],
})
export class AddToDashboardDialogComponent {
  chartForm: FormGroup;

  constructor(
    public dialogRef: MatDialogRef<AddToDashboardDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any,
    private formBuilder: FormBuilder,
    private apollo: Apollo,
    private router: Router
  ) {}

  ngOnInit() {
    this.chartForm = this.formBuilder.group({
      name: ['New Chart', Validators.required],
    });
  }

  submit() {
    if (!this.chartForm.valid) {
      return;
    }

    let mutation;
    const { itemId } = this.data;

    if (itemId) {
      mutation = gql`
        mutation updateDashboardItem($id: String!, $input: DashboardItemInput) {
          updateDashboardItem(id: $id, input: $input) {
            id
            name
          }
        }
      `;
    } else {
      mutation = gql`
        mutation createDashboardItem($input: DashboardItemInput) {
          createDashboardItem(input: $input) {
            id
            name
          }
        }
      `;
    }

    this.apollo
      .mutate({
        mutation,
        variables: {
          ...(itemId ? { id: itemId.toString() } : {}),
          input: {
            name: this.chartForm.value.name,
            vizState: JSON.stringify({
              query: this.data.cubeQuery,
              chartType: this.data.chartType,
              pivotConfig: this.data.pivotConfig,
            }),
            layout: '',
          },
        },
      })
      .subscribe(() => {
        this.dialogRef.close();
        this.router.navigate(['/dashboard']);
      });
  }

  onNoClick(): void {
    this.dialogRef.close();
  }
}
