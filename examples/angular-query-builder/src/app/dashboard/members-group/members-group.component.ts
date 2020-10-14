import { Component, EventEmitter, Input, Output } from '@angular/core';
import { MatSelectChange } from '@angular/material/select';

@Component({
  selector: 'members-group',
  templateUrl: './members-group.component.html',
  styleUrls: ['./members-group.component.css'],
})
export class MembersGroupComponent {
  @Input()
  title: string;

  @Input()
  members: any[];

  @Input()
  allMembers: any[];

  @Output()
  onSelect = new EventEmitter<string>();

  @Output()
  onMemberClick = new EventEmitter<string>();

  @Output()
  onReplace = new EventEmitter<any>();

  _onSelect(event: MatSelectChange) {
    this.onSelect.emit(event.value);
  }

  _onMemberClick(name: string) {
    this.onMemberClick.emit(name);
  }

  _onReplace(name: string, replaceWithName: string) {
    this.onReplace.emit({
      name,
      replaceWithName,
    });
  }
}
