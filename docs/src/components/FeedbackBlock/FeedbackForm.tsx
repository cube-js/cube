import React, { useState } from 'react';
import * as styles from './styles.module.scss';
import Button from '../Button'

const FeedbackForm = (props: propsType) => {
  const { setFeedbackMessage, clearFeedback, feedback } = props;
  const [message, setMessage] = useState('');

  const feedbackMessage = {
    like: 'Let us know what you like and how we can improve this page',
    dislike: 'Let us know how we can improve this page',
  };

  const handleSubmit = (
    event: Event,
    message: string,
  ) => {
    event.preventDefault();
    setFeedbackMessage(message || 'empty message');
  };

  return (
    <form className={styles.feedbackForm}>
      <textarea
        value={message}
        onChange={(e) => {
          setMessage(e.target.value);
        }}
        placeholder={feedbackMessage[feedback] || ''}
      ></textarea>
      <div className={styles.feedbackForm__buttons}>
        <Button
          view="primary"
          className={styles.feedbackForm__sendButton}
          onClick={(e: Event) => {
            handleSubmit(e, message);
          }}
          type="submit"
        >
          Send
        </Button>
        <Button view="outline" onClick={clearFeedback}>
          Cancel
        </Button>
      </div>
    </form>
  );
};

interface propsType {
  feedback: string;
  setFeedbackMessage: (message: string) => void;
  clearFeedback: () => void;
}

export default FeedbackForm;
