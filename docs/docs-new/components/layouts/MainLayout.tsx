import dynamic from 'next/dynamic';
import { Children } from 'react';

const FeedbackBlock = dynamic(() => import('@/components/common/FeedbackBlock/FeedbackBlock').then((r) => r.FeedbackBlock), {
  ssr: false
})

export const MainLayout = (props) => {
  const childrenAsArray = Children.toArray(props.children.props.children);
  const [firstChild, ...restChildren] = childrenAsArray;
  const childrenWithInjectedFeedback = [
    firstChild,
    // @ts-expect-error
    <FeedbackBlock key='feedback' />,
    ...restChildren,
  ];

  return (<>{childrenWithInjectedFeedback}</>);
};
