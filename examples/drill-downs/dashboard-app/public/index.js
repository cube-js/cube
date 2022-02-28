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

// dropdown menu accessibilty
function applyDropdownAccessibility() {
    const dropdownLinks = document.querySelectorAll(".dropdown-link")
    dropdownLinks.forEach(link => {
        link.addEventListener("focus", function () {
            dropdownMenuBtn.setAttribute("aria-expanded", true)
        })
    })

    const lastDropdownLinkItem = dropdownLinks.length - 1
    dropdownLinks[lastDropdownLinkItem].addEventListener("blur", function () {
        dropdownMenuBtn.setAttribute("aria-expanded", false)
    })
}

// dropdown menu functionality
const dropdownMenuBtn = document.querySelector(".dropdown-button")
const dropdownMenuList = document.querySelector(".menu-list")
dropdownMenuBtn.addEventListener("click", function (e) {
    // dropdownMenuList.scrollTop = 0;
    if (this.getAttribute("aria-expanded") === "true") {
        this.setAttribute("aria-expanded", false)
    } else {
        this.setAttribute("aria-expanded", true)
    }
})

// close dropdown when click outside
window.addEventListener("click", (e) => {
    if (!dropdownMenuBtn.contains(e.target)) {
        dropdownMenuBtn.setAttribute("aria-expanded", false)
    }
})

// mobile nav functionality
const navToggle = document.getElementById('nav-toggle');
const header = document.getElementById('header');
const navOverlay = document.getElementById('nav-overlay')

navToggle.addEventListener('click', (e) => {
    if (header.classList.contains('open')) {
        header.classList.remove('open');
        header.classList.add('hide');
        document.body.classList.toggle('noscroll')
    } else if (header.classList.contains('hide')) {
        header.classList.remove('hide');
        header.classList.add('open');
        document.body.classList.toggle('noscroll')
    } else {
        header.classList.add('open');
        document.body.classList.toggle('noscroll')
    }
})

navOverlay.addEventListener("click", ()=>{
    header.classList.remove('open');
    header.classList.add('hide');
    document.body.classList.toggle('noscroll')
})

// hide nav on window resize properly
window.addEventListener("resize", ()=>{
    const minDesktopWidth = this.getComputedStyle(document.documentElement)
                                .getPropertyValue("--breakpoint-desktop-xs")
                                .replace("px", "");

    if(this.innerWidth >= minDesktopWidth) {
        header.classList.remove('open');
        header.classList.remove('hide');
        dropdownMenuBtn.setAttribute("aria-expanded", false)
        document.body.classList.remove('noscroll')
    }
})



