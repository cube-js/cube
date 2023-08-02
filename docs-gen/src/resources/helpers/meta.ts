import { ProjectReflection } from 'typedoc';
import { CommentTag, ContainerReflection } from 'typedoc/dist/lib/models';

export function meta(this: ProjectReflection) {
  function findModuleRelection(reflection?: ContainerReflection) {
    if (!reflection) {
      return null;
    }

    if (reflection?.comment) {
      return reflection;
    }

    return findModuleRelection(reflection?.children?.[0]);
  }

  function tagConverter(tag: string)  {
    const tags = {
      menucategory: 'category',
      subcategory: 'subCategory',
      menuorder: 'menuOrder'
    };

    return tags[tag] ?? tag;
  }

  const moduleReflection = findModuleRelection(this);

  if (moduleReflection) {
    const { comment } = moduleReflection;
    const title = (comment?.tags || []).find((tag: CommentTag) => tag.tagName === 'title');
    const md = [`# ${title.text}`];
    const description = (comment?.tags || []).find((tag: CommentTag) => tag.tagName === 'description');

    if (description) {
      md.push('');
      md.push(description.text);
    }

    return md.join('\n');
  }

  return '';
}
