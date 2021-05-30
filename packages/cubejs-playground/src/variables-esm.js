const colors = {
  pink: '255, 100, 146',
  purple: '122, 119, 255',
  'purple-03': '175, 173, 255',
  'purple-04': '202, 201, 255',
  text: '91, 92, 125',
  'dark-01': '20, 20, 70',
  'dark-02': '67, 67, 107',
  'dark-03': '114, 114, 144',
  'dark-04': '161, 161, 181',
  'dark-05': '213, 213, 226',
  light: '243, 243, 251',
  green: '65, 181, 111',
  yellow: '251, 188, 5',
  gray: '246, 246, 248'
};

function color(name, opacity = 1) {
  return `rgba(${colors[name]}, ${opacity})`;
}

const VARIABLES = {
  'active-bg': color('purple', 0.1),
  'primary-bg': color('purple', 0.1),
  'primary-1': color('purple', 0.9),
  'primary-2': color('purple', 0.8),
  'primary-3': color('purple', 0.7),
  'primary-4': color('purple', 0.6),
  'primary-5': color('purple', 0.5),
  'primary-6': color('purple', 0.4),
  'primary-7': color('purple', 0.3),
  'primary-8': color('purple', 0.2),
  'primary-9': color('purple', 0.1),
  'primary-10': color('purple', 0),
  'light-5': color('light', 0.5),
  'remove-btn-bg': color('purple', 0.1),
  'remove-btn-hover-bg': color('purple', 0.2),
  'primary-color': color('purple'),

  'success-bg-color': color('green', 0.1),
  'success-color': color('green', 0.9),
  'warning-bg-color': color('yellow', 0.1),
  'warning-color': color('yellow', 0.9),

  'pink-8': color('pink', 0.2),
  'pink-9': color('pink', 0.1),

  'heading-color': color('dark', 0.65),
  'link-color': color('purple'),
  'info-color': color('purple'),
  'layout-body-background': '#f6f6f8;',
  'layout-header-background': '#eeeef5',
  'menu-highlight-color': color('dark-01'),
  'item-hover-bg': color('light'),
  'layout-header-height': '48px',

  'font-family':
    'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", "Avenir Next", Roboto, Oxygen-Sans, Ubuntu, Cantarell, "Helvetica Neue", sans-serif',
  'menu-item-font-size': '15px',
  'btn-primary-shadow': 'none',
  'btn-text-shadow': 'none',
  'modal-body-padding': '32px',
  'form-item-margin-bottom': '23px',

  'disabled-color': color('dark-01', 0.25),
  'disabled-bg': color('dark-05', 0.2),

  // menu
  'menu-item-active-bg': color('light'),

  'font-size-base': '14px',
  'border-radius-base': '4px',
  'padding-lg': '16px',
};

Object.keys(colors).forEach(
  (name) => (VARIABLES[`${name}-color`] = color(name))
);

const LESS_VARIABLES = {};
const CSS_PROPERTIES = {};

Object.keys(VARIABLES).forEach((key) => {
  LESS_VARIABLES[`@${key}`] = VARIABLES[key];
});

Object.keys(VARIABLES).forEach((key) => {
  CSS_PROPERTIES[`--${key}`] = VARIABLES[key];
});

export { VARIABLES, LESS_VARIABLES, CSS_PROPERTIES };
