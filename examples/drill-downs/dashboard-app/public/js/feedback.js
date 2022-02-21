const cubeTracking = require("cubedev-tracking")
const { event: cubeTrackingEvent } = cubeTracking


// feedback handler
const feedbackLikeBtn = document.getElementById("feedback-like")
const feedbackDislikeBtn = document.getElementById("feedback-dislike")
const feedbackBlock = document.getElementsByClassName("feedback")[0]
const thanksBlock = document.getElementsByClassName("thanks")[0]

function replaceFeedbackBlocks() {
    feedbackBlock.classList.add("d-none")
    thanksBlock.classList.remove("d-none")
}

function getFeedbackState(status) {
    return {
        page: window.location.host,
        date: new Date().toISOString(),
        feedback: status,
    }
}

feedbackLikeBtn.addEventListener("click", () => {
    // push vote to feedback log    
    const feedbackState = getFeedbackState("like");
    cubeTrackingEvent("example_feedback_like", feedbackState)
    replaceFeedbackBlocks()
})

feedbackDislikeBtn.addEventListener("click", () => {
    // push vote to feedback log
    const feedbackState = getFeedbackState("dislike")
    cubeTrackingEvent("example_feedback_like", feedbackState)
    replaceFeedbackBlocks()
})