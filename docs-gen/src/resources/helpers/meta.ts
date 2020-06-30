import { ProjectReflection } from 'typedoc';
import { CommentTag, ContainerReflection } from 'typedoc/dist/lib/models';

export function meta(this: ProjectReflection) {
  function findModuleRelection(reflection: ContainerReflection) {
    if (reflection.comment) {
      return reflection;
    }

    return findModuleRelection(reflection.children?.[0]);
  }

  const moduleReflection = findModuleRelection(this);

  if (moduleReflection) {
    const { comment } = moduleReflection;
    const md = ['---'];

    (comment?.tags || []).forEach((tag: CommentTag) => {
      if (tag.tagName !== 'description') {
        const escape = tag.tagName !== 'menuorder';
        const text = escape ? `'${tag.text}'` : tag.text;
        md.push(`${tag.tagName === 'menuorder' ? 'menuOrder' : tag.tagName}: ${text}`.replace('\n', ''));
      }
    });
    md.push('---');
    const description = (comment?.tags || []).find((tag: CommentTag) => tag.tagName === 'description');

    if (description) {
      md.push('');
      md.push(description.text);
    }

    return md.join('\n');
  }

  return '';
}
