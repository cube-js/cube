import React from 'react';
import kebabCase from 'lodash/kebabCase';

const MyH3 = (props) => {
  const startCommentIndex = props.children.indexOf('<--');
  const endCommentIndex = props.children.indexOf('-->');
  const isCustom = startCommentIndex !== -1 && endCommentIndex !== -1;

  if (isCustom) {
    const propsData = props.children?.slice(
      startCommentIndex + 3,
      endCommentIndex
    );

    if (propsData?.length) {
      const jsonProps = JSON.parse(propsData);
      const text = props.children.slice(endCommentIndex + 3);

      return (
        <h3
          id={kebabCase(jsonProps?.id) + '-' + kebabCase(text)}
          name={kebabCase(text)}
          {...props}
        >
          {text}
        </h3>
      );
    }
  }
  return <h3 name={kebabCase(props.children)} {...props} />;
};
export default MyH3;
