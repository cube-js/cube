import React, { useEffect, useState } from 'react';
import cubejs from '@cubejs-client/core';
const cubejsApi = cubejs(
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2MjUyNDczNzZ9.fSV2XCFB40j0Jvg1men1syKPFrbKWq8SC-7xqiB_G5I',
  { apiUrl: 'https://managing-aiea.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1' }
);
const query = {
  filters: [
    {
      member: 'Banner.start',
      operator: 'beforeDate',
      values: [formatDate(1)],
    },
    {
      member: 'Banner.end',
      operator: 'afterDate',
      values: [formatDate()],
    },
  ],
  dimensions: ['Banner.text', 'Banner.link', 'Banner.campaign'],
  limit: 1,
};
async function getBannerDataFromApi(set, setIsLoaded) {
  const resultSet = await cubejsApi.load(query);
  if (resultSet?.tablePivot()?.length) {
    const response = resultSet?.tablePivot?.()?.[0];
    if (response) {
      set(response);
      setIsLoaded(true);
      // set result to localStorage by date
      if (window?.localStorage) {
        window?.localStorage?.setItem(`website-banner-${formatDate()}`, JSON.stringify(response));
      }
    }
  }
  return resultSet;
}
function getBannerDataFromLocalStorage(set, setIsLoaded) {
  let item = window?.localStorage?.getItem(`website-banner-${formatDate()}`);
  set(JSON.parse(item));
  setIsLoaded('localStorage');
}
const EventBanner = (props) => {
  const [banner, setBanner] = useState(null);
  const [decoration, setDecoration] = useState('none');
  const [isLoaded, setIsLoaded] = useState(null);
  const [isMobile, setIsMobile] = useState(null);

  useEffect(() => {
    if (window?.localStorage && window.localStorage.getItem(`website-banner-${formatDate()}`)) {
      getBannerDataFromLocalStorage(setBanner, setIsLoaded);
    } else {
      getBannerDataFromApi(setBanner, setIsLoaded);
    }
    if (window?.screen?.availWidth && window?.screen?.availWidth < 640) {
      setIsMobile(true);
    }
  }, []);
  return (
    <a
      href={getLinkWithUTM(banner?.['Banner.link'], 'docs', banner?.['Banner.campaign'])}
      target="_blank"
      style={{
        paddingBottom: isLoaded ? (isMobile ? '54px' : '40px') : "0",
        color: 'rgb(255,255,255)',
        textDecoration: 'none',
        fontSize: '16px',
        fontWeight: '500',
        wordSpacing: '2px',
        display: isLoaded ? "block" : "auto",
        transition:  isLoaded === 'localStorage'
              ? null : 'padding 1s ease-in-out',
      }}
      onMouseEnter={() => setDecoration('underline')}
      onMouseLeave={() => setDecoration('none')}
    >
      <div
        style={{
          position: "fixed",
          width: "100%",
          zIndex: "99",
          textDecoration: decoration,
          minHeight: isLoaded ? (isMobile ? '54px' : '40px') : '0',
          maxHeight: isLoaded ? (isMobile ? '54px' : '40px') : '0',
          overflow: 'hidden',
          opacity: isLoaded ? '1' : '0',
          backgroundColor: 'rgb(122, 119, 255)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          lineHeight: 'normal',
          transition:
            isLoaded === 'localStorage'
              ? null
              : 'max-height 1s ease-in-out, opacity 1s ease-in-out, padding 1s ease-in-out',
          padding: isLoaded ? '7px 16px' : "0 16px",
        }}
      >
        {banner?.['Banner.text']}
      </div>
    </a>
  );
};
export default EventBanner;
function formatDate(daysToAdd) {
  let d = new Date();

  if (daysToAdd) {
    d.setDate(d.getDate() + daysToAdd);
  }

  let month = '' + (d.getMonth() + 1);
  let day = '' + d.getDate();
  let year = d.getFullYear();

  if (month.length < 2) month = '0' + month;
  if (day.length < 2) day = '0' + day;

  return [year, month, day].join('-');
}
function getLinkWithUTM(link, source, compagin) {
  if (!link) {
    return null;
  }
  const lastSymbol = link.charAt(link.length - 1);
  const utm = `?utm_campaign=${compagin}&utm_medium=purple&utm_source=${source}`;
  if (lastSymbol !== '/') {
    return link + '/' + utm;
  }
  return link + utm;
}