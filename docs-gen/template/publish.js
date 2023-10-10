const fs = require('fs-extra');
const inline = require('jsdoc/tag/inline');
const inflection = require('inflection');

let typeDefs = [];
let knownClassNames = [];

const anchorName = (link) => inflection.dasherize(inflection.underscore(link.replace(/#/g, '-')));

const resolveInlineLinks = (str) => inline.replaceInlineTags(str, {
  link: (string, { completeTag, text }) => string.replace(completeTag, `[${text}](#${anchorName(text)})`),
}).newString;

const renderLinks = (p) => {
  if (p.type.names[0] === '*') {
    return '*';
  }
  if (p.type && knownClassNames.indexOf(p.type.names.join('#')) !== -1) {
    return `[${p.type.names.join('#')}](#${anchorName(p.type.names.join('-'))})`;
  }
  if (p.type) {
    return `\`${p.type.names.join('|')}\``;
  }
  return p;
};

function generateParams(doclet, field = 'params') {
  const params = doclet[field].map((p) => {
    const optional = p.optional ? '**Optional**' : null;
    const defaultValue = p.defaultvalue ? `**Default:** \`${p.defaultvalue}\`` : null;
    const type = p.type && p.type.parsedType &&
    (p.type.parsedType.name || p.type.parsedType.parsedExpression || p.type.parsedType.typeExpression);
    const formattedType = type ? `: ${type}` : '';
    const options = [optional, defaultValue].filter((f) => !!f);

    if (!p.description && typeDefs.find((td) => td.name === type)) {
      p.description = `See {@link ${type}}`;
    }

    p.description = (p.description || '').replace(/\n/g, ' ').trim();

    return `- \`${p.name}${formattedType}\`${options.length ? ` (${options.join(', ')})` : ''}${
      p.description ? ` - ${resolveInlineLinks(p.description)}` : ''
    }`;
  });

  if (field === 'properties') {
    return `**Properties:**\n\n${params.join('\n')}\n`;
  }

  return `**Parameters:**\n\n${params.join('\n')}\n`;
}

function generateTypeDefs(doclets) {
  if (!doclets.length) {
    return '';
  }

  const res = doclets.map((doclet) => [`## ${doclet.name}`, doclet.description, generateParams(doclet, 'properties'), '\n']
    .filter((d) => !!d)
    .join('\n'));

  return res.join('\n');
}

const generateFunctionDocletSection = (doclet, isConstructor) => {
  const title = doclet.name;
  const header = `##${doclet.longname.indexOf('#') !== -1 || isConstructor ? '#' : ''} ${title}${
    isConstructor ? ' Constructor' : ''
  }\n`;
  const args =
    (doclet.params &&
      doclet.params
        .filter((p) => p.name.indexOf('.') === -1)
        .map((p) => (p.optional ? `[${p.name}]` : p.name))
        .join(', ')) ||
    '';
  const signature = `\`${isConstructor ? 'new ' : ''}${doclet.meta.code.name || doclet.name}(${args})\`\n`;
  const params = doclet.params ? generateParams(doclet) : '';
  const returns = doclet.returns ?
    `**Returns:** ${doclet.returns.map(
      (p) => `${p.type ? renderLinks(p) : ''}${p.description ? ` ${resolveInlineLinks(p.description)}` : ''}`
    )}` :
    '';
  return [header, signature, doclet.description && `${resolveInlineLinks(doclet.description)}\n`, params, returns, '\n']
    .filter((f) => !!f)
    .join('\n');
};

const generateClassSection = (doclet) => {
  const header = `## ${doclet.name}\n`;
  let classSection = [header, (doclet.classdesc || doclet.description).trim(), '\n'].join('\n');
  if (doclet.params && doclet.params.length) {
    classSection = classSection.concat(generateFunctionDocletSection(doclet, true));
  }
  return classSection;
};

const tagValue = (doclet, tagOriginalTitle) => {
  const tag = doclet.tags && doclet.tags.find((t) => t.originalTitle === tagOriginalTitle);
  return tag && tag.value;
};

const generateModuleSection = (doclet) => `# ${doclet.name}

${doclet.description}\n\n`;

const generateMarkDown = (doclets, parent) => {
  if (!parent) {
    const rootModule = doclets.find((d) => d.kind === 'module' && d.description);
    return generateModuleSection(rootModule).concat(generateMarkDown(doclets, rootModule));
  }
  const children = doclets.filter((d) => d.memberof === parent.longname);
  const order = (doclet) => parseInt(tagValue(doclet, 'order'), 10) || 0;
  children.sort((a, b) => order(a) - order(b));
  return children
    .map((child) => {
      if (child.kind === 'class') {
        return generateClassSection(child)
          .concat(generateMarkDown(doclets, child))
          .concat(generateTypeDefs(typeDefs.filter((td) => td.memberof === child.name)));
      } else if (child.kind === 'function' || child.kind === 'member') {
        return generateFunctionDocletSection(child);
      }
      return null;
    })
    .filter((markdown) => !!markdown)
    .join('');
};

const classNamesFrom = (doclets) => doclets.filter((d) => d.kind === 'class').map((d) => d.name);

exports.publish = (data, { destination }) => {
  knownClassNames = classNamesFrom(data().get());
  typeDefs = data()
    .get()
    .filter((d) => d.kind === 'typedef');

  const markDown = generateMarkDown(
    data()
      .get()
      .filter((d) => !d.undocumented && d.kind !== 'typedef')
  );
  fs.writeFile(destination, markDown);
};
