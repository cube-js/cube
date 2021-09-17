import React from 'react';
import kebabCase from 'lodash/kebabCase';
import ScrollLink from '../templates/ScrollSpyLink';
import * as styles from '../../../static/styles/index.module.scss';
import cx from 'classnames';
import { Icon } from 'antd';

const ScrollSpyH3 = (props) => {
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
      const hash = kebabCase(text);
      const id = kebabCase(jsonProps?.id) + '-' + kebabCase(text);

      return (
        <h3
          id={kebabCase(jsonProps?.id) + '-' + kebabCase(text)}
          name={hash}
          className={styles.hTag}
          {...props}
        >
          <ScrollLink
            activeClass={styles.scrollspyCurrent}
            to={id || hash}
            key={hash + Math.random()}
            className={cx(styles.scrollspyLink, styles.scrollspySubitem)}
          >
            <Icon type="link" className={styles.hTagIcon} />
            {text}
          </ScrollLink>
        </h3>
      );
    }
  }
  return <h3 name={kebabCase(props.children)} {...props} />;
};
export default ScrollSpyH3;
