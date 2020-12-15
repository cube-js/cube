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
};

function color(name, opacity = 1) {
  return `rgba(${colors[name]}, ${opacity})`;
}

module.exports = {
  'active-bg': color('purple', .1),
  'primary-bg': color('purple', .1),
  'primary-1': color('purple', .9),
  'primary-2': color('purple', .8),
  'primary-3': color('purple', .7),
  'primary-4': color('purple', .6),
  'primary-5': color('purple', .5),
  'primary-6': color('purple', .4),
  'primary-7': color('purple', .3),
  'primary-8': color('purple', .2),
  'primary-9': color('purple', .1),
  'primary-10': color('purple', 0),
  'light-5': color('light', .5),
  'remove-btn-bg': color('purple', .1),
  'remove-btn-hover-bg': color('purple', .2),
  'primary-color': color('purple'),

  'heading-color': color('dark', .65),
  'link-color': color('purple'),
  'info-color': color('purple'),
  'layout-body-background': '#f3f3fc',
  'layout-header-background': '#eeeef5',
  'menu-highlight-color': color('dark-01'),
  'item-hover-bg': color('light'),
  'layout-header-height': '48px',

  'font-family': 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", "Avenir Next", Roboto, Oxygen-Sans, Ubuntu, Cantarell, "Helvetica Neue", sans-serif',
  'menu-item-font-size': '15px',
  'btn-primary-shadow': 'none',
  'btn-text-shadow': 'none',
  'modal-body-padding': '32px',
  'form-item-margin-bottom': '23px',
  'disabled-color': color('dark-04'),
  'disabled-bg': color('dark-05', .2),

  // menu
  'menu-item-active-bg': color('light'),

  'font-size-base': '14px',
  'border-radius-base': '4px',
  'padding-lg': '16px',
};

Object.keys(colors)
  .forEach((name) => {
    module.exports[`${name}-color`] = color(name);
  });
