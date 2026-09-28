import Link from 'next/link';
import styles from './Footer.module.css';
import { CubeLogo } from './CubeLogo';
import { SocialIcon } from './SocialIcon';

const CURRENT_YEAR = new Date().getFullYear();

const COMPANY_LINKS = [
  { label: 'About', link: 'https://cube.dev/about' },
  { label: 'Careers', link: 'https://cube.dev/careers' },
  { label: 'Terms of Use', link: 'https://cube.dev/terms-of-use' },
  { label: 'Privacy Policy', link: 'https://cube.dev/privacy-policy' },
  { label: 'Security Statement', link: 'https://cube.dev/security' },
  { label: 'List of Subprocessors', link: 'https://cube.dev/subprocessors' },
];

const RESOURCES_LINKS = [
  { label: 'Docs', link: 'https://cube.dev/docs/' },
  { label: 'Blog', link: 'https://cube.dev/blog/' },
  { label: 'Community', link: 'https://cube.dev/community' },
  { label: 'Events', link: 'https://cube.dev/events' },
  { label: 'Customer Stories', link: 'https://cube.dev/case-studies' },
  { label: 'Consulting Partners', link: 'https://cube.dev/consulting/consulting-partners' },
];

const CHANNEL_LINKS = {
  github: { url: 'https://github.com/cube-js/cube.js', label: 'GitHub Repositories' },
  slack: { url: 'https://slack.cube.dev/', label: 'Slack community' },
  twitter: { url: 'https://twitter.com/the_cube_dev', label: 'Twitter account' },
  youtube: { url: 'https://www.youtube.com/channel/UC5jQrtiI85SUs9zj6FhdkfQ', label: 'Youtube channel' },
  stackoverflow: { url: 'https://stackoverflow.com/questions/tagged/cube.js', label: 'StackOverflow questions' },
  linkedin: { url: 'https://www.linkedin.com/company/cube-dev/', label: 'LinkedIn Profile' },
};

const FooterLink = ({ href, children, className, ...props }) => {
  if (href.startsWith('/')) {
    return (
      <Link href={href} className={`${styles.Link} ${className || ''}`} {...props}>
        {children}
      </Link>
    );
  }

  return (
    <a href={href} className={`${styles.Link} ${className || ''}`} {...props}>
      {children}
    </a>
  );
};

export const Footer = () => {
  return (
    <footer className={styles.Footer}>
      <div className={styles.Footer__content}>
        <div className={`${styles.Column} ${styles['Column--first']}`}>
          <CubeLogo />
          <span className={styles.Copyright}>© {CURRENT_YEAR} Cube Dev, Inc.</span>
        </div>

        <div className={`${styles.Column} ${styles['Column--second']}`}>
          <span className={styles.Column__Title}>Resources</span>
          {RESOURCES_LINKS.map((link) => (
            <FooterLink key={link.link} href={link.link}>
              {link.label}
            </FooterLink>
          ))}
        </div>

        <div className={`${styles.Column} ${styles['Column--third']}`}>
          <span className={styles.Column__Title}>Company</span>
          {COMPANY_LINKS.map((link) => (
            <FooterLink key={link.link} href={link.link}>
              {link.label}
            </FooterLink>
          ))}
        </div>

        <div className={`${styles.Column} ${styles['Column--fourth']}`}>
          <span className={styles.Column__Title}>Channels</span>
          <div className={styles.ChannelLinks}>
            {Object.keys(CHANNEL_LINKS).map((type) => {
              const link = CHANNEL_LINKS[type];
              return (
                <a
                  className={styles.ChannelLink}
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
          className={styles.Status}
          target="_blank"
          rel="noreferrer"
        >
          Cube Cloud Status
          <div className={styles.Status__dot} />
        </FooterLink>
      </div>
    </footer>
  );
};
