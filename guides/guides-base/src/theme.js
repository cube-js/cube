import { css } from 'styled-components'

const theme = {
  contentPageMaxWidth: '1100px',
  landingMaxWidth: '1261px',
  landingPadding: '80px',
  landingPaddingLessThenLarge: '30px',
  fontFamily: `'DM Sans', sans-serif`,

  colors: {
    grey: "#A1A1B5",
    lightBlue: '#F3F3FB',
    purple: '#43436B',
    darkPurple: '#141446',
    red: '#FF6492'
  }
};

export default theme;

export const sharedStyles = {
  button: css`
    text-decoration: none;
    display: inline-block;
    border-radius: 4px;
    font-size: 20px;
    font-weight: bold;
    padding: 20px 55px;
    transition: background-color 0.2s linear;
  `,
  markdown: css`
    font-family: ${theme.fontFamily}

    h1, h2, h3, h4, h5, h6, h7 {
      color: ${theme.colors.purple};
    }

    h2 {
      margin-top: 40px;
      font-size: 26px;
    }

    p {
      line-height: 28px;
    }

    p, ul, ol {
      color: #727290;
      font-size: 16px;
    }

    .gatsby-highlight {
      margin-top: 25px;
      margin-bottom: 25px;
    }

    a:not(.anchor) {
      color: #6f76d9;
      text-decoration: none;
      border-bottom: 1px solid rgba(111,118,217,.3);
      transition: border .2s;

      &:hover {
        color: #6f76d9;
        border-color: rgba(111,118,217,.8);
      }
    }

    // TODO: should put into prism styles
    pre {
      font-size: 13px;
    }


   p > code[class*="language-"] {
      padding: 3px 6px;
      line-height: 16px;
      font-size: 13px;
      border-radius: 4px;
      border: 1px solid #ececf0;
      font-weight: 500;
      letter-spacing: .02em;
      background-color: #fafafa;
      color: #50556c;
    }

    img, video {
      max-width: 100%;
      border: 1px solid #ececf0;
      border-radius: 4px;
    }
  `
};
