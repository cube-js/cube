'use client'

import { useState } from 'react'
import { FeedbackForm } from './FeedbackForm'
import styles from './FeedbackBlock.module.css'

// Dynamically import cubedev-tracking to avoid SSR issues
const trackEvent = async (eventName, data) => {
  if (typeof window !== 'undefined') {
    const { event } = await import('cubedev-tracking')
    event(eventName, data)
  }
}

const LikeIcon = () => (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M4.66667 14.6667H2.66667C2.31305 14.6667 1.97391 14.5262 1.72386 14.2761C1.47381 14.0261 1.33334 13.687 1.33334 13.3333V8.66667C1.33334 8.31305 1.47381 7.97391 1.72386 7.72386C1.97391 7.47381 2.31305 7.33334 2.66667 7.33334H4.66667M9.33334 6.00001V3.33334C9.33334 2.80291 9.12262 2.29421 8.74755 1.91913C8.37248 1.54406 7.86377 1.33334 7.33334 1.33334L4.66667 7.33334V14.6667H12.1867C12.5083 14.6703 12.8203 14.5577 13.0653 14.3494C13.3103 14.1411 13.4717 13.8517 13.52 13.5333L14.44 7.53334C14.469 7.34178 14.4561 7.14616 14.4022 6.96001C14.3483 6.77386 14.2548 6.60166 14.1283 6.45554C14.0018 6.30942 13.8452 6.19282 13.6693 6.11354C13.4934 6.03426 13.3024 5.99418 13.1093 5.99334H9.33334V6.00001Z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
  </svg>
)

const DislikeIcon = () => (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M11.3333 1.33334H13.3333C13.687 1.33334 14.0261 1.47381 14.2761 1.72386C14.5262 1.97391 14.6667 2.31305 14.6667 2.66667V7.33334C14.6667 7.68696 14.5262 8.0261 14.2761 8.27615C14.0261 8.5262 13.687 8.66667 13.3333 8.66667H11.3333M6.66667 10V12.6667C6.66667 13.1971 6.87738 13.7058 7.25245 14.0809C7.62753 14.456 8.13623 14.6667 8.66667 14.6667L11.3333 8.66667V1.33334H3.81334C3.49174 1.3297 3.17971 1.44228 2.93472 1.65062C2.68973 1.85895 2.52831 2.14829 2.48001 2.46667L1.56001 8.46667C1.53098 8.65823 1.54387 8.85385 1.59778 9.04C1.65169 9.22615 1.74524 9.39835 1.87172 9.54447C1.9982 9.69059 2.15479 9.80719 2.33072 9.88647C2.50664 9.96575 2.69763 10.0058 2.89067 10.0067H6.66667V10Z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
  </svg>
)

export function FeedbackBlock() {
  const [date, setDate] = useState('')
  const [feedback, setFeedbackState] = useState('')
  const [isShowThanks, setShowThanks] = useState(false)

  const setFeedback = (state) => {
    if (feedback) {
      return
    }
    const page = typeof window !== 'undefined' ? window.location.pathname : ''
    const newDate = new Date().toISOString()
    setDate(newDate)
    setFeedbackState(state)
    trackEvent('page_feedback_like', { page, date: newDate, feedback: state })
  }

  const setFeedbackMessage = (message) => {
    const page = typeof window !== 'undefined' ? window.location.pathname : ''
    if (message) {
      trackEvent('page_feedback_comment', { page, date, feedback, comment: message })
    }
    setShowThanks(true)
  }

  const clearFeedback = () => {
    setFeedbackState('')
  }

  if (isShowThanks) {
    return (
      <div className={styles.ThanksBlock}>Thank you for the feedback!</div>
    )
  }

  return (
    <div className={styles.FeedbackBlock}>
      <div className={styles.FeedbackBlock__wrap}>
        <p className={styles.FeedbackBlock__question}>
          Was this page useful?
        </p>
        <div className={styles.ButtonGroup}>
          <button
            className={`${styles.Button} ${styles.success}`}
            onClick={() => setFeedback('like')}
          >
            <LikeIcon />
            <span>Yes</span>
          </button>
          <button
            className={`${styles.Button} ${styles.danger}`}
            onClick={() => setFeedback('dislike')}
          >
            <DislikeIcon />
            <span>No</span>
          </button>
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
  )
}
