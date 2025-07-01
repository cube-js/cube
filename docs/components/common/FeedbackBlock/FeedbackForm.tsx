import React, { useState } from 'react';
import styles from './FeedbackForm.module.css';
import { Button, ButtonGroup } from '../Button/Button';

// const Button = (props) => <button {...props} />;

const FeedbackForm = (props: propsType) => {
  const { setFeedbackMessage, clearFeedback, feedback } = props;
  const [message, setMessage] = useState('');

  const feedbackMessage = {
    like: 'Let us know what you like and how we can improve this page',
    dislike: 'Let us know how we can improve this page',
  };

  const handleSubmit = (
    event: React.MouseEvent<HTMLButtonElement>,
    message: string,
  ) => {
    event.preventDefault();
    setFeedbackMessage(message);
  };

  return (
    <form className={styles.FeedbackForm}>
      <textarea
        value={message}
        onChange={(e) => {
          setMessage(e.target.value);
        }}
        placeholder={feedbackMessage[feedback] || ''}
      ></textarea>
      <ButtonGroup className={styles.FeedbackForm__buttons}>
        <Button
          className={styles.FeedbackForm__sendButton}
          onClick={(e) => {
            handleSubmit(e, message);
          }}
          type="submit"
        >
          Send
        </Button>
        <Button variant="secondary" onClick={clearFeedback}>
          Cancel
        </Button>
      </ButtonGroup>
    </form>
  );
};

interface propsType {
  feedback: string;
  setFeedbackMessage: (message: string) => void;
  clearFeedback: () => void;
}

export default FeedbackForm;
