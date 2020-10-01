import { TableColumn } from '@cubejs-client/core';

export function getDisplayedColumns(tableColumns: TableColumn[]): string[] {
  const queue = tableColumns;
  const columns = [];

  while (queue.length) {
    const column = queue.shift();
    if (column.dataIndex) {
      columns.push(column.dataIndex);
    }
    if ((column.children || []).length) {
      column.children.map((child) => queue.push(child));
    }
  }

  return columns;
}

export function flattenColumns(columns: TableColumn[] = []) {
  return columns.reduce((memo, column) => {
    const titles = flattenColumns(column.children);
    return [
      ...memo,
      ...(titles.length
        ? titles.map((title) => column.shortTitle + ', ' + title)
        : [column.shortTitle]),
    ];
  }, []);
}
