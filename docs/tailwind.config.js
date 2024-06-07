const makePrimaryColor =
  (l) =>
  ({ opacityValue }) => {
    if (opacityValue === undefined) {
      return `hsl(var(--nextra-primary-hue) 100% ${l}%)`;
    }
    return `hsl(var(--nextra-primary-hue) 100% ${l}% / ${opacityValue})`;
  };

/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: ["class", 'html[class~="dark"]'],
  theme: {
    extend: {
      colors: {
        primary: {
          50: makePrimaryColor(97),
          100: makePrimaryColor(94),
          200: makePrimaryColor(86),
          300: makePrimaryColor(77),
          400: makePrimaryColor(66),
          500: makePrimaryColor(50),
          600: makePrimaryColor(45),
          700: makePrimaryColor(39),
          750: makePrimaryColor(35),
          800: makePrimaryColor(32),
          900: makePrimaryColor(24),
        },
      },
      fontFamily: {
        sans: ["var(--font)"],
        mono: ["var(--font-mono)"],
      },
    },
  },
  plugins: [],
};
