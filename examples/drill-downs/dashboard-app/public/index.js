const minDesktopWidth = getComputedStyle(document.documentElement)
                            .getPropertyValue("--breakpoint-desktop-xs")
                            .replace("px", "");

const menuList = document.getElementById("menu-list")
const menuCurrent = document.getElementById("menu-current")
const menuButton = document.getElementById("menu-button")

// fetch examples nav items
const NAV_CONFIG_PATH = "examples-nav.config.json";
// fetch nav items from config file


const loadNavItems = () => fetch(NAV_CONFIG_PATH).then(res => res.json())
.then(data => populateExamplesNav(data))
.catch();

setTimeout(()=>loadNavItems(), 700)

const populateExamplesNav = (data) => {
    // find current nav item index
    const currentNavItemIndex = data.map(item => item.url).indexOf(window.location.href)
    // remove current nav item from data
    const currentNavItem = data.splice(currentNavItemIndex, 1)[0]

    // generate nav options from data items
    const navItems = data
        .map(item =>
            `<li class="dropdown-list-item"><a class="dropdown-link" href="${item.url}">${item.name}</a></li>`)
        .join("");

    // remove loader
    menuButton.classList.toggle("load")
    // set options to menu select
    menuList.innerHTML = navItems
    // set current item name as menu button text
    menuCurrent.innerHTML = currentNavItem.name

    // apply dropdown accessibility only when dropdown-list-items are rendered
    applyDropdownAccessibility()

    // if there is more then 8 menu items
    // set such a height so that the user understands 
    // that it is possible to scroll down
    if (data.length > 7 && window.innerWidth >= minDesktopWidth) {
        menuList.style.maxHeight = '430px'
    }

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

navOverlay.addEventListener("click", () => {
    header.classList.remove('open');
    header.classList.add('hide');
    document.body.classList.toggle('noscroll')
})

window.addEventListener("resize", () => {
    if (this.innerWidth >= minDesktopWidth) {
        // hide nav on window resize properly
        header.classList.remove('open');
        header.classList.remove('hide');
        dropdownMenuBtn.setAttribute("aria-expanded", false)
        document.body.classList.remove('noscroll')

        // fix menu max-height
        // if there is more then 8 menu items
        // set such a height so that the user understands 
        // that it is possible to scroll down
        if (menuList.childNodes.length > 7) {
            menuList.style.maxHeight = '430px'
        }

    }else{
        menuList.style.maxHeight = '100%'
    }
})



