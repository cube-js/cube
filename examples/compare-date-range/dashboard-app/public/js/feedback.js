const cubeTracking = require('cubedev-tracking');
const { event: cubeTrackingEvent } = cubeTracking;

// controls
const feedbackLikeBtn = document.querySelector('#feedback-like');
const feedbackDislikeBtn = document.querySelector('#feedback-dislike');
const feedbackMessageField = document.querySelector('#feedback-message');
const feedbackMessageSendBtn = document.querySelector('#feedback-message-send');
const feedbackMessageCancelBtn = document.querySelector('#feedback-message-cancel');

// UI
const feedbackBlock = document.querySelector('.Feedback__block');
const feedbackMessageForm = document.querySelector('.Feedback__message-form');
const thanksBlock = document.querySelector('.Feedback__thanks');

const FEEDBACK_LIKE = 'like';
const FEEDBACK_DISLIKE = 'dislike';
const feedbackMessagePlaceholder = {
  [FEEDBACK_LIKE]: 'Let us know what you like and how we can improve this page',
  [FEEDBACK_DISLIKE]: 'Let us know how we can improve this page',
};

let feedback = ''; // selected feedback status: like or dislike

const submitFeedbackLike = (status) => {
  // submit event
  feedback = status;
  cubeTrackingEvent('page_feedback_like', { feedback });

  showFeedbackMessageForm();
};

feedbackLikeBtn.addEventListener('click', (e) => {
  if (feedback) return;

  submitFeedbackLike(FEEDBACK_LIKE);

  feedbackLikeBtn.setAttribute('active', true);
  feedbackDislikeBtn.disabled = true;
});

feedbackDislikeBtn.addEventListener('click', () => {
  if (feedback) return;

  submitFeedbackLike(FEEDBACK_DISLIKE);

  feedbackDislikeBtn.setAttribute('active', true);
  feedbackLikeBtn.disabled = true;
});

feedbackMessageSendBtn.addEventListener('click', (e) => {
  e.preventDefault();

  // send cube tracking event
  const comment = feedbackMessageField.value;
  cubeTrackingEvent('page_feedback_comment', { feedback, comment });

  showThanks();
});

feedbackMessageCancelBtn.addEventListener('click', (e) => {
  e.preventDefault();

  showThanks();
});

function showThanks() {
  feedbackBlock.classList.add('d-none');
  feedbackMessageForm.classList.add('d-none');
  thanksBlock.classList.remove('d-none');
}

function showFeedbackMessageForm() {
  // update message form UI
  feedbackMessageField.value = '';
  feedbackMessageField.placeholder = feedbackMessagePlaceholder[feedback];
  feedbackMessageForm.classList.remove('d-none');
}
