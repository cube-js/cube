import kebabCase from 'lodash/kebabCase';

export const getHashFromContent = (input) => {
  const isString = typeof input === 'string';
  const isArray = Array.isArray(input);

  let nameString = '';

  if (isArray) {
    nameString = input[0] + input[1].props.children;
  }
  else if (isString) {
    nameString = input;
  }
  else {
    nameString = input.props.children;
  }

  return kebabCase(nameString);
}
