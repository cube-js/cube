import classnames from 'classnames/bind';

import * as styles from './CaseStudyPromoBlock.module.scss';

const cn = classnames.bind(styles);

interface CaseStudyPromoBlockProps {
  title: React.ReactNode;
  children: React.ReactNode;
}

export const CaseStudyPromoBlock = ({
  title,
  children
}) => {

  return (
    <div className={cn('CaseStudyPromoBlock__Wrapper')}>
      <div className={cn('CaseStudyPromoBlock__Title')}>
        {title}
      </div>
      <div className={cn('CaseStudyPromoBlock__Text')}>
        {children}
      </div>
    </div>
  );
}
