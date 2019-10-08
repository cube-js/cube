const fs = require('fs-extra');
const inline = require('jsdoc/tag/inline');

let knownClassNames = [];

const resolveInlineLinks = str => {
  return inline.replaceInlineTags(str, {
    link: (string, { completeTag, text, tag }) => string.replace(completeTag, `[${text}](${text})`)
  }).newString;
};

const renderLinks = (p) => {
  if (p.type.names[0] === '*') {
    return '*';
  }
  if (p.type && knownClassNames.indexOf(p.type.names.join('#')) !== -1) {
    return `[${p.type.names.join('#')}](#${p.type.names.join('-')})`;
  }
  if (p.type) {
    return `**${p.type.names.join('#')}**`;
  }
  return p;
};

const generateFunctionDocletSection = (doclet) => {
  const title = doclet.name;
  const header = `##${doclet.longname.indexOf('#') !== -1 ? '#' : ''} ${title}\n`;
  const signature = `> ${doclet.meta.code.name}(${doclet.params && doclet.params.filter(p => p.name.indexOf('.') === -1).map(p => p.name).join(', ') || ''})\n`;
  const params = doclet.params ? `**Parameters:**\n\n${doclet.params.map(p => `- **${p.name}**${p.description ? ` - ${p.description}` : ''}`).join('\n')}\n` : ``;
  const returns = doclet.returns ? `**Returns:** ${doclet.returns.map(p => `${p.type ? renderLinks(p) : ''}${p.description ? ` ${resolveInlineLinks(p.description)}` : ''}`)}` : ``;
  return [header, signature, `${doclet.description}\n`, params, returns, '\n'].join('\n');
};

const generateClassSection = (doclet) => {
  const header = `## ${doclet.name}\n`;
  return [header, (doclet.classdesc || doclet.description).trim(), '\n'].join('\n');
};

const tagValue = (doclet, tagOriginalTitle) => {
  const tag = doclet.tags && doclet.tags.find(t => t.originalTitle === tagOriginalTitle);
  return tag && tag.value;
};

const generateModuleSection = (doclet) => {
  return `---
title: '${doclet.name}'
permalink: ${tagValue(doclet, 'permalink')}
category: ${tagValue(doclet, 'category')}
subCategory: Reference
menuOrder: ${tagValue(doclet, 'menuOrder')}
---

${doclet.description}\n\n`;
};

const generateMarkDown = (doclets, parent) => {
  if (!parent) {
    const rootModule = doclets.find(d => d.kind === 'module' && d.description);
    return generateModuleSection(rootModule).concat(generateMarkDown(doclets, rootModule));
  }
  const children = doclets.filter(d => d.memberof === parent.longname);
  const order = (doclet) => parseInt(tagValue(doclet, 'order'), 10) || 0;
  children.sort((a, b) => order(a) - order(b));
  return children.map(child => {
    if (child.kind === 'class') {
      return generateClassSection(child).concat(generateMarkDown(doclets, child));
    } else if (child.kind === 'function' || child.kind === 'member') {
      return generateFunctionDocletSection(child);
    }
    return null;
  }).filter(markdown => !!markdown).join('');
};

const classNamesFrom = (doclets) => {
  return doclets.filter(d => d.kind === 'class').map(d => d.name);
};

exports.publish = (data, { destination }) => {
  knownClassNames = classNamesFrom(data().get());
  const markDown = generateMarkDown(data().get().filter(d => !d.undocumented));
  fs.writeFile(destination, markDown);
};
