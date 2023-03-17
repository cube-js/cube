import kebabCase from 'lodash/kebabCase';

export const getHashFromContent = (children) => {
  const isString = typeof children === 'string';
  const nameString = isString ? children : children.props.children;
  return kebabCase(nameString);
}
