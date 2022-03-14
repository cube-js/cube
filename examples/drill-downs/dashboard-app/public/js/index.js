// CSS
const minDesktopWidth = getComputedStyle(document.documentElement)
  .getPropertyValue('--breakpoint-desktop-xs')
  .replace('px', '');

const menuList = document.querySelector('#menu-list');

// dropdown menu functionality
const dropdownMenuBtn = document.querySelector('.dropdown-button');
const dropdownMenuList = document.querySelector('.menu-list');
dropdownMenuBtn.addEventListener('click', function (e) {
  dropdownMenuList.scrollTop = 0;
  if (this.getAttribute('aria-expanded') === 'true') {
    this.setAttribute('aria-expanded', false);
  } else {
    this.setAttribute('aria-expanded', true);
  }
});

// close dropdown when click outside
window.addEventListener('click', (e) => {
  if (!dropdownMenuBtn.contains(e.target)) {
    dropdownMenuBtn.setAttribute('aria-expanded', false);
  }
});

// mobile nav functionality
const navToggle = document.querySelector('#nav-toggle');
const header = document.querySelector('#header');
const navOverlay = document.querySelector('#nav-overlay');

const hideNav = () => {
  header.classList.remove('open');
  header.classList.add('hide');
  document.body.classList.toggle('noscroll');
  navToggle.setAttribute('aria-expanded', false);
};

const showNav = () => {
  header.classList.remove('hide');
  header.classList.add('open');
  document.body.classList.toggle('noscroll');
  navToggle.setAttribute('aria-expanded', true);
};

navToggle.addEventListener('click', (e) => {
  if (header.classList.contains('open')) {
    hideNav();
  } else if (header.classList.contains('hide')) {
    showNav();
  } else {
    showNav();
  }
});

navOverlay.addEventListener('click', () => {
  if (header.classList.contains('open')) {
    hideNav();
  }
});

window.addEventListener('resize', () => {
  if (this.innerWidth >= minDesktopWidth) {
    // hide nav on window resize properly
    header.classList.remove('open');
    header.classList.remove('hide');
    dropdownMenuBtn.setAttribute('aria-expanded', false);
    document.body.classList.remove('noscroll');
    navToggle.setAttribute('aria-expanded', false);

    // fix menu max-height
    // if there is more then 8 menu items
    // set such a height so that the user understands
    // that it is possible to scroll down
    if (menuList.childNodes.length > 7) {
      menuList.classList.add('overflow');
    }
  } else {
    menuList.classList.remove('overflow');
  }
});
