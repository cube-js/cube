const cubeTracking = require("cubedev-tracking")
const { event: cubeTrackingEvent } = cubeTracking

// fetch examples nav items
const navConfigPath = "nav.config.json";
const populateExamplesNav = (data) => {
    // generate nav options from data items
    const navOptions = data
        .map(item =>
            `<li class="dropdown-list-item"><a href="${item.url}">${item.name}</a></li>`)
        .join("");

    // set options to menu select
    const menu = document.getElementsByClassName("menu-list")[0]
    menu.innerHTML = navOptions
}

fetch(navConfigPath).then(res => res.json())
    .then(data => populateExamplesNav(data))
    .catch();

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