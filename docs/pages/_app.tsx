import { type AppProps } from "next/app";

import "@/styles/globals.css";
import "@/styles/palette.css";
import "@/styles/typography.css";
import "@/styles/math.css";
import "@/styles/images.css";
import "katex/dist/katex.min.css";
import "@cube-dev/marketing-ui/dist/index.css";

import PurpleBanner from '@cube-dev/purple-banner';
import localFont from "next/font/local";
import { Inter } from "next/font/google";
import { SearchProvider } from "@cube-dev/marketing-ui";
import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';

export const SourceCodePro = localFont({
  src: "../fonts/SourceCodePro-Regular.woff2",
  weight: "400",
  style: "normal",
});

export const JetBrainsMono = localFont({
  src: "../fonts/JetBrainsMono-Regular.woff2",
  weight: "400",
  style: "normal",
});

const inter = Inter({
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
});

export const CeraPro = localFont({
  src: [
    {
      path: "../fonts/CeraPro-Regular.woff2",
      weight: "300",
      style: "normal",
    },
    {
      path: "../fonts/CeraPro-Regular.woff2",
      weight: "400",
      style: "normal",
    },
    {
      path: "../fonts/CeraPro-Medium.woff2",
      weight: "500",
      style: "normal",
    },
    {
      path: "../fonts/CeraPro-Bold.woff2",
      weight: "600",
      style: "normal",
    },
    {
      path: "../fonts/CeraPro-Bold.woff2",
      weight: "700",
      style: "normal",
    },
  ],
});

type Props = { origin: string | null };

export default function MyApp({ origin, Component, pageProps }: AppProps & Props) {
  const router = useRouter()
  const [pbVisible, setPbVisible] = useState(false)

  useEffect(() => {
    requestAnimationFrame(() => setPbVisible(true))
  }, [])

  // Track page views
  useEffect(() => {
    const handleRouteChange = async (url) => {
      if (typeof window !== 'undefined') {
        const { page } = await import('cubedev-tracking');
        page();
      }
    }

    router.events.on('routeChangeStart', handleRouteChange)

    // If the component is unmounted, unsubscribe
    // from the event with the `off` method:
    return () => {
      router.events.off('routeChangeStart', handleRouteChange)
    }
  }, [router])

  return (
    <SearchProvider
      algoliaAppId={process.env.NEXT_PUBLIC_ALGOLIA_APP_ID}
      algoliaApiKey={process.env.NEXT_PUBLIC_ALGOLIA_API_KEY}
      algoliaIndexName={process.env.NEXT_PUBLIC_ALGOLIA_INDEX_NAME}
    >
      <style jsx global>{`
        :root {
          --font: ${inter.style.fontFamily};
          --font-title: ${CeraPro.style.fontFamily};
          --font-mono: ${JetBrainsMono.style.fontFamily};
          --font-code: ${SourceCodePro.style.fontFamily};
          --cube-font: ${CeraPro.style.fontFamily};
          --purple-banner-font: ${CeraPro.style.fontFamily};
        }
        .pb-wrapper {
          max-height: 0;
          overflow: hidden;
          transition: max-height 0.3s linear;
          will-change: max-height;
        }
        .pb-wrapper--visible {
          max-height: 100px;
        }
        @media (max-width: 796px) {
          .pb-wrapper--visible {
            max-height: 160px;
          }
        }
      `}</style>

      <div className={pbVisible ? 'pb-wrapper pb-wrapper--visible' : 'pb-wrapper'}>
        <PurpleBanner
          utmSource="cube.dev"
          debugMode={process.env.NEXT_PUBLIC_SHOW_PURPLE_BANNER === 'true'}
        />
      </div>

      <Component {...pageProps} />
    </SearchProvider>
  );
}
