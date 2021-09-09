import React, { useState } from 'react';
import * as styles from './styles.module.scss';
import Button from '../Button'

const FeedbackForm = (props: propsType) => {
  const { setFeedbackMessage, clearFeedback } = props;
  const [message, setMessage] = useState('');

  const handleSubmit = (
    event: Event,
    message: string,
  ) => {
    event.preventDefault();
    if (!message) {
      return;
    }
    setFeedbackMessage(message);
  };

  return (
    <form className={styles.feedbackForm}>
      <textarea
        value={message}
        onChange={(e) => {
          setMessage(e.target.value);
        }}
        placeholder="Let us know what you like and how we can improve this page"
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
  setFeedbackMessage: (message: string) => void;
  clearFeedback: () => void;
}

export default FeedbackForm;
