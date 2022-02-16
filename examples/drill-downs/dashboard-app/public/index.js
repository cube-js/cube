const cubeTracking = require("cubedev-tracking")
const { event: cubeTrackingEvent } = cubeTracking

// fetch examples nav items
const NAV_CONFIG_PATH = "examples-nav.config.json";
const populateExamplesNav = (data) => {
    const menu = document.getElementsByClassName("menu-list")[0]
    const menuButton = document.getElementById("menu-button")

    // find current nav item index
    const currentNavItemIndex = data.map(item=>item.url).indexOf(window.location.href)
    // remove current nav item from data
    const currentNavItem = data.splice(currentNavItemIndex, 1)[0]

    // generate nav options from data items
    const navItems = data
        .map(item =>
            `<li class="dropdown-list-item"><a href="${item.url}">${item.name}</a></li>`)
        .join("");

    // set options to menu select
    menu.innerHTML = navItems
    // set current item name as menu button text
    menuButton.innerHTML = currentNavItem.name
}

// fetch nav items from config file
fetch(NAV_CONFIG_PATH).then(res => res.json())
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