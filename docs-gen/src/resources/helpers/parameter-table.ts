import { ParameterReflection } from 'typedoc';

import MarkdownTheme from '../../theme';
import { stripLineBreaks } from './strip-line-breaks';
import paramTypeToString from './param-type-to-string';

const escape = (s: string) => {
  const lookup = {
      '&': '&amp;',
      '"': '&quot;',
      '\'': '&apos;',
      '<': '&lt;',
      '>': '&gt;',
      '|': '&#124;'
  };
  const regex = new RegExp(`[${Object.keys(lookup).join('')}]`, 'g');
  return s.replace( regex, c => lookup[c] );
}

const wrapInCodeTags = (s) => `<code class='nx-border-black nx-border-opacity-[0.04] nx-bg-opacity-[0.03] nx-bg-black nx-break-words nx-rounded-md nx-border nx-py-0.5 nx-px-[.25em] nx-text-[.9em] dark:nx-border-white/10 dark:nx-bg-white/10'>${s}</code>`

export function parameterTable(this: ParameterReflection[], hideUncommented: boolean) {
  const md = [];
  const defaultValues = this.map((param) => !!param.defaultValue);
  const hasDefaultValues = !defaultValues.every((value) => !value);

  const comments = this.map(
    (param) => (param.comment && !!param.comment.text) || (param.comment && !!param.comment.shortText)
  );
  const hasComments = !comments.every((value) => !value);

  const headers = ['Name', 'Type'];

  if (hasDefaultValues) {
    headers.push('Default');
  }

  if (hasComments) {
    headers.push('Description');
  }

  if (hideUncommented && !hasComments) {
    return '';
  }

  if (hideUncommented) {
    md.push('**Parameters:**\n');
  }

  const rows = this.map((parameter) => {
    const isOptional = parameter.flags.includes('Optional');

    const typeOut = paramTypeToString(parameter);

    const row = [
      wrapInCodeTags(`${parameter.flags.isRest ? '...' : ''}${parameter.name}${isOptional ? '?' : ''}`),
      typeOut ? wrapInCodeTags(escape(typeOut.toString())) : '',
    ];
    if (hasDefaultValues) {
      row.push(parameter.defaultValue ? parameter.defaultValue : '-');
    }
    if (hasComments) {
      const commentsText = [];
      if (parameter.comment && parameter.comment.shortText) {
        commentsText.push(
          MarkdownTheme.handlebars.helpers.comment.call(stripLineBreaks.call(parameter.comment.shortText))
        );
      }
      if (parameter.comment && parameter.comment.text) {
        commentsText.push(MarkdownTheme.handlebars.helpers.comment.call(stripLineBreaks.call(parameter.comment.text)));
      }
      row.push(commentsText.length > 0 ? commentsText.join(' ') : '-');
    }
    return `| ${row.join(' | ')} |\n`;
  });

  md.push(`\n| ${headers.join(' | ')} |\n| ${headers.map(() => '------').join(' | ')} |\n${rows.join('')}`);

  return md.join('');
}
