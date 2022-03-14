const cubejs = require('@cubejs-client/core').default;

// DOM
const menuList = document.querySelector('#menu-list');
const menuCurrent = document.querySelector('#menu-current');
const menuButton = document.querySelector('#menu-button');

// CSS
const minDesktopWidth = getComputedStyle(document.documentElement)
  .getPropertyValue('--breakpoint-desktop-xs')
  .replace('px', '');

// TODO: move credentials to env
const cubejsApi = cubejs(
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NDcyNzEzMzh9.ovHUQRSNDW3h1d9fCnNpr20sr3Gpj89F6aPZwkFEER0',
  { apiUrl: 'https://relevant-badger.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1' }
);

const createQuery = () => ({
  dimensions: ['ExamplesMeta.name', 'ExamplesMeta.url'],
  timeDimensions: [],
  order: {
    'ExamplesMeta.name': 'asc',
  },
  filters: [],
});

const mapDataFormat = (data) =>
  data.map((item) => ({
    name: item['ExamplesMeta.name'],
    url: item['ExamplesMeta.url'],
  }));

function fetchData() {
  return cubejsApi
    .load(createQuery())
    .then((res) => res.rawData())
    .then((data) => mapDataFormat(data))
    .then((formattedData) => populateExamplesNav(formattedData));
}

fetchData();

const populateExamplesNav = (data) => {
  // find current nav item index
  const currentNavItemIndex = data.map((item) => item.url).indexOf(window.location.href);
  if (currentNavItemIndex === -1) {
    // if there is no current item in list
    // set plug as current item name
    // for local dev and preview purposes
    menuCurrent.innerHTML = 'Check other examples';
  } else {
    // remove current nav item from data
    const currentNavItem = data.splice(currentNavItemIndex, 1)[0];
    // set current item name as menu button text
    menuCurrent.innerHTML = currentNavItem.name;
  }

  // generate nav options from data items
  const navItems = data
    .map((item) => `<li class="dropdown-list-item"><a class="dropdown-link" href="${item.url}">${item.name}</a></li>`)
    .join('');

  // remove loader
  menuButton.classList.toggle('load');
  // set options to menu select
  menuList.innerHTML = navItems;

  // apply dropdown accessibility only when dropdown-list-items are rendered
  applyDropdownAccessibility();

  // if there is more then 8 menu items
  // set such a height so that the user understands
  // that it is possible to scroll down
  if (data.length > 7 && window.innerWidth >= minDesktopWidth) {
    // menuList.style.maxHeight = menuOverflowMaxHeight
    menuList.classList.add('overflow');
  }
};

// dropdown menu accessibilty
function applyDropdownAccessibility() {
  const dropdownLinks = document.querySelectorAll('.dropdown-link');
  dropdownLinks.forEach((link) => {
    link.addEventListener('focus', function () {
      dropdownMenuBtn.setAttribute('aria-expanded', true);
    });
  });

  const lastDropdownLinkItem = dropdownLinks.length - 1;
  dropdownLinks[lastDropdownLinkItem].addEventListener('blur', function () {
    dropdownMenuBtn.setAttribute('aria-expanded', false);
  });
}
