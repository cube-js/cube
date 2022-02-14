// feedback handler
const feedbackLikeBtn = document.getElementById("feedback-like")
const feedbackDislikeBtn = document.getElementById("feedback-dislike")
const feedbackBlock = document.getElementsByClassName("feedback")[0]
const thanksBlock = document.getElementsByClassName("thanks")[0]

function replaceFeedbackBlocks(){
    feedbackBlock.classList.add("d-none")
    thanksBlock.classList.remove("d-none")
}

// add like event listener
feedbackLikeBtn.addEventListener("click", ()=>{
    console.log('you selected like')
    replaceFeedbackBlocks()
})

// add dislike event listener
feedbackDislikeBtn.addEventListener("click", ()=>{
    console.log('you selected dislike')
    replaceFeedbackBlocks()
})