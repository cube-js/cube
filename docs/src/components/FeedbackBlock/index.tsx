import React, { useState } from 'react';
import { event } from 'cubedev-tracking';
import * as styles from './styles.module.scss';
import Button from "../Button"
import FeedbackForm from './FeedbackForm'

const FeedbackBlock = (props: propsType) => {
  const { page } = props;
  const [date, setDate] = useState('');
  const [feedback, setFeedbackState] = useState('');
  const [feedbackMessage, setFeedbackMessage] = useState('');

  const setFeedback = (state: string, page: string) => {
    const date = new Date().toISOString();
    setDate(date);
    event('page-feedback-like', { page, date, feedback });
    setFeedbackState(state);
  };

  if (feedbackMessage) {
    return <h1>Thank you!</h1>;
  }
  return (
    <div className={styles.feedbackBlock}>
      <div className={styles.feedbackBlock__wrap}>
        <p className={styles.feedbackBlock__question}>
          Did you find this page useful?
        </p>
        <div className={styles.feedbackBlock__buttons}>
          <Button
            active={feedback === 'like' ? 'active' : null}
            disabled={feedback === 'dislike' ? 'disabled' : null}
            type="like"
            onClick={() => setFeedback('like', page)}
          >
            Yes
          </Button>
          <Button
            active={feedback === 'dislike' ? 'active' : null}
            disabled={feedback === 'like' ? 'disabled' : null}
            type="dislike"
            onClick={() => setFeedback('dislike', page)}
          >
            No
          </Button>
        </div>
      </div>
      {feedback && date && (
        <FeedbackForm setFeedbackMessage={setFeedbackMessage} date={date} />
      )}
    </div>
  );
};

interface propsType {
  page: string;
}

export default FeedbackBlock;
