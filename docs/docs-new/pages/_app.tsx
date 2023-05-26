import { type AppProps } from "next/app";

import "@/styles/globals.css";
import "@/styles/palette.css";
import "@/styles/typography.css";
import "@/styles/math.css";
import "@/styles/images.css";
import "katex/dist/katex.min.css";
import "@cube-dev/marketing-ui/dist/index.css";

import localFont from "next/font/local";
import { Inter } from "next/font/google";
import { SearchProvider } from "@cube-dev/marketing-ui";

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

export default function MyApp({ Component, pageProps }: AppProps) {
  return (
    <SearchProvider
      algoliaAppId={process.env.NEXT_PUBLIC_ALGOLIA_APP_ID}
      algoliaApiKey={process.env.NEXT_PUBLIC_ALGOLIA_API_KEY}
      algoliaIndexName={process.env.NEXT_PUBLIC_ALGOLIA_INDEX_NAME}
      baseUrl="/"
    >
      <style jsx global>{`
        :root {
          --font: ${inter.style.fontFamily};
          --font-title: ${CeraPro.style.fontFamily};
          --font-mono: ${JetBrainsMono.style.fontFamily};
          --font-code: ${SourceCodePro.style.fontFamily};
          --cube-font: ${CeraPro.style.fontFamily};
        }
      `}</style>
      <Component {...pageProps} />
    </SearchProvider>
  );
}
