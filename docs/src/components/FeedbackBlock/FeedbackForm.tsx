import React, { useState } from 'react';
import * as styles from './styles.module.scss';
import Button from '../Button'

const FeedbackForm = (props: propsType) => {
  const { date, setFeedbackMessage } = props;

  return (
    <div className={styles.feedbackForm}>
      <textarea placeholder="Let us know what you like and how we can improve this page"></textarea>
      <div className={styles.feedbackForm__buttons}>
        <Button type="primary">Send</Button>
        <Button type="secondary">Cancel</Button>
      </div>
    </div>
  );
};

interface propsType {
  date: string;
  setFeedbackMessage: React.Dispatch<React.SetStateAction<string>>;
}

export default FeedbackForm;
