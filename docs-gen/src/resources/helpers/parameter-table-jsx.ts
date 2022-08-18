import { ParameterReflection } from 'typedoc';

import paramTypeToString from './param-type-to-string';

const printJson = (input) => JSON.stringify(input, null, 2);

export function parameterTableJsx(this: ParameterReflection[], hideUncommented: boolean) {

  const defaultValues = this.map((param) => !!param.defaultValue);
  const hasDefaultValues = !defaultValues.every((value) => !value);

  const comments = this.map(
    (param) => (param.comment && !!param.comment.text) || (param.comment && !!param.comment.shortText)
  );
  const hasComments = !comments.every((value) => !value);

  const columns = [
    'Name',
    'Type',
  ];

  if (hasDefaultValues) {
    columns.push('Default');
  }

  if (hasComments) {
    columns.push('Description');
  }

  if (hideUncommented && !hasComments) {
    return '';
  }

  const data = this.map((parameter) => {
    const isOptional = parameter.flags.includes('Optional');

    const paramName = `${parameter.flags.isRest ? '...' : ''}${parameter.name}${isOptional ? '?' : ''}`;
    const typeOut = paramTypeToString(parameter);
    const paramType = typeOut
      ? typeOut.toString()
      : '';
    const commentsText = [];

    if (hasComments) {
      if (parameter.comment && parameter.comment.shortText) {
        commentsText.push(
          parameter.comment.shortText
        );
      }
      if (parameter.comment && parameter.comment.text) {
        parameter.comment.text
      }
    }

    return {
      Name: paramName,
      Type: paramType,
      Default: parameter.defaultValue ? parameter.defaultValue : '-',
      Description: commentsText.join(''),
    };
  });

  return `

<ParameterTable
  columns={${printJson(columns)}}
  data={${printJson(data)}}
  opts={${printJson({
    hideUncommented,
  })}}
/>
`;
}
