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
      values: [formatDate()],
    },
    {
      member: 'Banner.end',
      operator: 'afterDate',
      values: [formatDate()],
    },
  ],
  dimensions: ['Banner.text', 'Banner.link'],
  limit: 1,
};
async function getBannerDataFromApi(set, setIsLoaded) {
  const resultSet = await cubejsApi.load(query);

  if (resultSet?.tablePivot()?.length) {
    set(resultSet.tablePivot()[0]);
    setIsLoaded(true);
  }
  return resultSet;
}
const EventBanner = (props) => {
  const [banner, setBanner] = useState(null);
  const [decoration, setDecoration] = useState('none');
  const [isLoaded, setIsLoaded] = useState(null);
  const [isMobile, setIsMobile] = useState(null);
  useEffect(() => {
    if (window?.screen?.availWidth && window?.screen?.availWidth < 768) {
      setIsMobile(true);
    }
    getBannerDataFromApi(setBanner, setIsLoaded);
  }, []);
  return (
    <a
      href={getLinkWithUTM(banner?.['Banner.link'], 'docs')}
      target="_blank"
      style={{
        paddingBottom: isLoaded ? (isMobile ? '80px' : '40px') : "0",
        color: 'rgb(255,255,255)',
        textDecoration: 'none',
        fontSize: isMobile ? '16px' : '18px',
        fontWeight: '500',
        display: isLoaded ? "block" : "auto",
        transition: 'padding 1s linear',
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
          minHeight: isLoaded ? (isMobile ? '80px' : '40px') : '0',
          opacity: isLoaded ? '1' : '0',
          backgroundColor: 'rgb(122, 119, 255)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          lineHeight: 'normal',
          transition: 'min-height 1s linear, opacity 1s linear, padding 1s linear',
          textOverflow: "ellipsis",
          padding: isLoaded ? '7px 16px' : "0 16px",
        }}
      >
        {banner?.['Banner.text']}
      </div>
    </a>
  );
};
export default EventBanner;
function formatDate() {
  var d = new Date(),
    month = '' + (d.getMonth() + 1),
    day = '' + d.getDate(),
    year = d.getFullYear();
  if (month.length < 2) month = '0' + month;
  if (day.length < 2) day = '0' + day;
  return [year, month, day].join('-');
}
function getLinkWithUTM(link, source) {
  if (!link) {
    return null;
  }
  const lastSymbol = link.charAt(link.length - 1);
  const utm = `?utm_campaign=${formatDate().replaceAll('-', '')}&utm_medium=purple&utm_source=${source}`;
  if (lastSymbol === '/') {
    return link.substring(0, link.length - 1) + utm;
  }
  return link + utm;
}