import React, { useState } from 'react';
import { event } from 'cubedev-tracking';
import * as styles from './styles.module.scss';
import Button from "../Button"
import FeedbackForm from './FeedbackForm'

const FeedbackBlock = (props: propsType) => {
  const { page} = props;
  const [date, setDate] = useState('');
  const [feedback, setFeedbackState] = useState('');
  const [isShowThanks, setShowThanks] = useState(false);

  const setFeedback = (state: string, page: string) => {
    if (feedback) {
      return
    }
    const date = new Date().toISOString();
    setDate(date);
    setFeedbackState(state);
    console.log({ page, date, feedback: state })
    // event('page_feedback_like', { page, date, feedback: state });
  };
  const setFeedbackMessage = (message: string) => {
    if (message) {
      event('page_feedback_comment', { page, date, feedback, comment: message });
    }
    setShowThanks(true);
  };
  const clearFeedback = () => {
    setFeedbackState('');
  }

  if (isShowThanks) {
    return (
      <div className={styles.thanksBlock}>Thank you for the feedback!</div>
    );
  }

  return (
    <div className={styles.feedbackBlock}>
      <div className={styles.feedbackBlock__wrap}>
        <p className={styles.feedbackBlock__question}>
          Did you find this page useful?
        </p>
        <div className={styles.feedbackBlock__buttons}>
          <Button
            className={styles.feedbackBlock__like}
            active={feedback === 'like' ? 'active' : null}
            disabled={feedback === 'dislike' ? 'disabled' : null}
            view="like"
            onClick={() => setFeedback('like', page)}
          >
            Yes
          </Button>
          <Button
            active={feedback === 'dislike' ? 'active' : null}
            disabled={feedback === 'like' ? 'disabled' : null}
            view="dislike"
            onClick={() => setFeedback('dislike', page)}
          >
            No
          </Button>
        </div>
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

interface propsType {
  page?: string;
}

export default FeedbackBlock;
