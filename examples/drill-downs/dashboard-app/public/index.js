// fetch examples nav items
const NAV_CONFIG_PATH = "examples-nav.config.json";

// fetch nav items from config file
fetch(NAV_CONFIG_PATH).then(res => res.json())
    .then(data => populateExamplesNav(data))
    .catch();

const populateExamplesNav = (data) => {
    const menu = document.getElementsByClassName("menu-list")[0]
    const menuButton = document.getElementById("menu-button")

    // find current nav item index
    const currentNavItemIndex = data.map(item => item.url).indexOf(window.location.href)
    // remove current nav item from data
    const currentNavItem = data.splice(currentNavItemIndex, 1)[0]

    // generate nav options from data items
    const navItems = data
        .map(item =>
            `<li class="dropdown-list-item"><a class="dropdown-link" href="${item.url}">${item.name}</a></li>`)
        .join("");

    // set options to menu select
    menu.innerHTML = navItems
    // set current item name as menu button text
    menuButton.innerHTML = currentNavItem.name

    // apply dropdown accessibility only when dropdown-list-items are rendered
    applyDropdownAccessibility()
}

// dropdown menu functionality
function applyDropdownAccessibility() {
    const dropdownMenuBtn = document.querySelector(".dropdown-button")
    dropdownMenuBtn.addEventListener("focus", function () {
        this.setAttribute("aria-expanded", true)
    })
    dropdownMenuBtn.addEventListener("blur", function () {
        this.setAttribute("aria-expanded", false)
    });
    
    const dropdownLinks = document.querySelectorAll(".dropdown-link")
    dropdownLinks.forEach(link => {
        link.addEventListener("focus", function(){
            dropdownMenuBtn.setAttribute("aria-expanded", true)
        })
    })

    const lastDropdownLinkItem = dropdownLinks.length - 1
    dropdownLinks[lastDropdownLinkItem].addEventListener("blur", function(){
        dropdownMenuBtn.setAttribute("aria-expanded", false)
    })
}


