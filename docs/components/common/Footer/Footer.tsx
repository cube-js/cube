import type {
  AnchorHTMLAttributes,
  DetailedHTMLProps,
  FC,
  PropsWithChildren,
} from "react";
import Link from "next/link";

import classes from "./Footer.module.css";
import classnames from "classnames/bind";
import {
  SocialIcon,
  SocialIconProps,
} from "@/components/common/SocialIcon/SocialIcon";
import { CubeLogo } from "@/components/common/CubeLogo";
const cn = classnames.bind(classes);

const CURRENT_YEAR = new Date().getFullYear();

const COMPANY_LINKS = [
  {
    label: "About",
    link: "https://cube.dev/about",
  },
  {
    label: "Careers",
    link: "https://cube.dev/careers",
  },
  {
    label: "Terms of Use",
    link: "https://cube.dev/terms-of-use",
  },
  {
    label: "Privacy Policy",
    link: "https://cube.dev/privacy-policy",
  },
  {
    label: "Security Statement",
    link: "https://cube.dev/security",
  },
  {
    label: "List of Subprocessors",
    link: "https://cube.dev/subprocessors",
  },
];

const RESOURCES_LINKS = [
  {
    label: "Docs",
    link: "https://cube.dev/docs/",
  },
  {
    label: "Blog",
    link: "https://cube.dev/blog/",
  },
  {
    label: "Community",
    link: "https://cube.dev/community",
  },
  {
    label: "Events",
    link: "https://cube.dev/events",
  },
  {
    label: "Customer Stories",
    link: "https://cube.dev/case-studies",
  },
  {
    label: "Cube Partner Network",
    link: "https://cube.dev/consulting/cube-partner-network",
  },
];

const CHANNEL_LINKS = {
  github: {
    url: "https://github.com/cube-js/cube.js",
    label: "GitHub Repositories",
  },
  slack: {
    url: "https://slack.cube.dev/",
    label: "Slack community",
  },
  twitter: {
    url: "https://twitter.com/the_cube_dev",
    label: "Twitter account",
  },
  youtube: {
    url: "https://www.youtube.com/channel/UC5jQrtiI85SUs9zj6FhdkfQ",
    label: "Youtube channel",
  },
  stackoverflow: {
    url: "https://stackoverflow.com/questions/tagged/cube.js",
    label: "StackOverflow questions",
  },
  linkedin: {
    url: "https://www.linkedin.com/company/cube-dev/",
    label: "LinkedIn Profile",
  },
};

interface FooterLinkProps
  extends DetailedHTMLProps<
    AnchorHTMLAttributes<HTMLAnchorElement>,
    HTMLAnchorElement
  > {
  className?: string;
  href: string;
}

const FooterLink: FC<PropsWithChildren<FooterLinkProps>> = ({
  href,
  children,
  className,
  ...props
}) => {
  if (href.startsWith("/")) {
    return (
      <Link href={href} passHref legacyBehavior>
        <a {...props} className={cn("Link", className)}>
          {children}
        </a>
      </Link>
    );
  }

  return (
    <a {...props} href={href} className={cn("Link", className)}>
      {children}
    </a>
  );
};

export interface FooterProps {
  className?: string;
  border?: "top";
}

export const Footer: FC<FooterProps> = ({ border, className }) => {
  return (
    <footer className={cn("Footer", "Footer--borderTop")}>
      <div
        className={cn(
          "Footer__content",
          "max-w-[90rem] m-auto pl-[max(env(safe-area-inset-left),1.5rem)] pr-[max(env(safe-area-inset-right),1.5rem)]"
        )}
      >
        <div className={cn("Column", "Column--first")}>
          <CubeLogo />
          {/* <LogoImage alt="Cube Dev logo" className={classes.Logo} /> */}
          <span className={classes.Copyright}>
            © {CURRENT_YEAR} Cube Dev, Inc.
          </span>
        </div>

        <div className={cn("Column", "Column--second")}>
          <span className={classes.Column__Title}>Resources</span>
          {RESOURCES_LINKS.map((link) => (
            <FooterLink key={link.link} href={link.link}>
              {link.label}
            </FooterLink>
          ))}
        </div>

        <div className={cn("Column", "Column--third")}>
          <span className={classes.Column__Title}>Company</span>
          {COMPANY_LINKS.map((link) => (
            <FooterLink key={link.link} href={link.link}>
              {link.label}
            </FooterLink>
          ))}
        </div>

        <div className={cn("Column", "Column--fourth")}>
          <span className={classes.Column__Title}>Channels</span>
          <div className={classes.ChannelLinks}>
            {Object.keys(CHANNEL_LINKS).map((type: SocialIconProps["type"]) => {
              const link = CHANNEL_LINKS[type];
              return (
                <a
                  className={classes.ChannelLink}
                  key={link.label}
                  href={link.url}
                  target="_blank"
                  rel="noreferrer"
                  aria-label={link.label}
                >
                  <SocialIcon type={type} />
                </a>
              );
            })}
          </div>
        </div>

        <FooterLink
          href="https://status.cubecloud.dev/"
          className={classes.Status}
          target="_blank"
          rel="noreferrer"
        >
          Cube Cloud Status
          <div className={classes.Status__dot} />
        </FooterLink>
      </div>
    </footer>
  );
};
