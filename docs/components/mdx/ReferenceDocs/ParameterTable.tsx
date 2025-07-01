import React from 'react';

export interface Column {
  Name: string;
  Type: string;
  Default?: string;
  Description?: string;
}

export interface ParameterTableProps {
  columns: (keyof Column)[];
  data: Column[];
  opts: {
    hideUncommented: boolean;
  }
}

export const ParameterTable = (props: ParameterTableProps) => {
  const { columns, data = [], opts } = props;

  return (
    <>
      {opts.hideUncommented
        ? (<strong>Parameters:</strong>)
        : null}

      <table>
        <tr>
          {columns.map((column) => (
            <th key={column as string}>{column}</th>
          ))}
        </tr>
        {data.map((item)=> (
          <tr key={item.Name}>
            <td>
              <code>{item.Name}</code>
            </td>
            <td>
              <code>{item.Type}</code>
            </td>
            {columns.includes('Default') && (<td>{item.Default}</td>)}
            {columns.includes('Description') && (<td>{item.Description}</td>)}
          </tr>
        ))}
      </table>
    </>
  );
}
