import React, { useState } from 'react';
import { event } from 'cubedev-tracking';
import { ButtonGroup, DislikeButton, LikeButton } from '@/components/common/Button/Button';
import FeedbackForm from '@/components/common/FeedbackBlock/FeedbackForm';

import styles from './FeedbackBlock.module.css';

export interface FeedbackBlockProps {
}

export const FeedbackBlock = (props: FeedbackBlockProps) => {
  const page = window.location.pathname;
  const [date, setDate] = useState('');
  const [feedback, setFeedbackState] = useState('');
  const [isShowThanks, setShowThanks] = useState(false);

  const setFeedback = (state: string, page: string) => {
    if (feedback) {
      return;
    }
    const date = new Date().toISOString();
    setDate(date);
    setFeedbackState(state);
    event('page_feedback_like', { page, date, feedback: state });
  };
  const setFeedbackMessage = (message: string) => {
    if (message) {
      event('page_feedback_comment', { page, date, feedback, comment: message });
    }
    setShowThanks(true);
  };
  const clearFeedback = () => {
    setFeedbackState('');
  };

  if (isShowThanks) {
    return (
      <div className={styles.ThanksBlock}>Thank you for the feedback!</div>
    );
  }

  return (
    <div className={styles.FeedbackBlock}>
      <div className={styles.FeedbackBlock__wrap}>
        <p className={styles.FeedbackBlock__question}>
          Was this page useful?
        </p>
        <ButtonGroup>
          <LikeButton
            // className={styles.FeedbackBlock__like}
            // active={feedback === 'like' ? 'active' : null}
            // disabled={feedback === 'dislike' ? 'disabled' : null}
            // view='like'
            onClick={() => setFeedback('like', page)}
          >
            Yes
          </LikeButton>
          <DislikeButton
            // active={feedback === 'dislike' ? 'active' : null}
            // disabled={feedback === 'like' ? 'disabled' : null}
            // view='dislike'
            onClick={() => setFeedback('dislike', page)}
          >
            No
          </DislikeButton>
        </ButtonGroup>
      </div>
      {feedback && date && (
        <FeedbackForm
          feedback={feedback}
          setFeedbackMessage={setFeedbackMessage}
          clearFeedback={clearFeedback}
        />
      )}
    </div>
  );
};
export default FeedbackBlock;
