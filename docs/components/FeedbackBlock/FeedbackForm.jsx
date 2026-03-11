'use client'

import { useState } from 'react'
import styles from './FeedbackForm.module.css'

const feedbackMessage = {
  like: 'Let us know what you like and how we can improve this page',
  dislike: 'Let us know how we can improve this page',
}

export function FeedbackForm({ setFeedbackMessage, clearFeedback, feedback }) {
  const [message, setMessage] = useState('')

  const handleSubmit = (e) => {
    e.preventDefault()
    setFeedbackMessage(message)
  }

  return (
    <form className={styles.FeedbackForm}>
      <textarea
        value={message}
        onChange={(e) => setMessage(e.target.value)}
        placeholder={feedbackMessage[feedback] || ''}
      />
      <div className={styles.FeedbackForm__buttons}>
        <button
          className={styles.Button}
          onClick={handleSubmit}
          type="submit"
        >
          Send
        </button>
        <button
          className={`${styles.Button} ${styles.secondary}`}
          onClick={clearFeedback}
          type="button"
        >
          Cancel
        </button>
      </div>
    </form>
  )
}
