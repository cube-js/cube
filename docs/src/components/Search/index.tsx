import React, { Component } from 'react';
import Helmet from 'react-helmet';
import cx from 'classnames';

import * as styles from '../../../static/styles/index.module.scss';
import '../../../static/styles/docsearch.scss';
import '../../../static/styles/docsearch-custom.css';

const ALGOLIA_PATH_PREFIX = '/cubejs/docs';

// @TODO Find a way to move this out of here
declare global {
  const docsearch: any;

  interface Window {
    analytics: any;
  }
}

type Props = {
  onClose(): void;
  navigate(url: string): void;
  mobile?: boolean;
};

const defaultProps = {
  mobile: false,
};

export default class Search extends Component<Props> {
  state = {
    open: false,
  };

  searchInput: any;

  constructor(props: Props) {
    super({ ...defaultProps, ...props });
  }

  /**
   * Replace the default selection event, allowing us to do client-side
   * navigation thus avoiding a full page refresh.
   *
   * Ref: https://github.com/algolia/autocomplete.js#events
   */
  autocompleteSelected = (e: Event) => {
    e.stopPropagation();
    // Use an anchor tag to parse the absolute url (from autocomplete.js) into a relative url
    const a = document.createElement(`a`);
    a.href = (e as Event & { _args: { url: string }[] })._args[0].url;
    this.searchInput.blur();
    this.searchInput.value = '';
    e.stopPropagation();
    this.props.navigate(
      `${a.pathname.replace(ALGOLIA_PATH_PREFIX, '/docs')}${a.hash}`
    );
  };

  componentDidMount() {
    this.props.mobile && this.searchInput.focus();
    window.addEventListener(
      `autocomplete:selected`,
      this.autocompleteSelected,
      true
    );
    // eslint-disable-next-line no-undef
    docsearch({
      apiKey: process.env.ALGOLIA_API_KEY,
      indexName: process.env.ALGOLIA_INDEX_NAME,
      inputSelector: '#search',
      debug: false,
      layout: `simple`,
      autocompleteOptions: {
        openOnFocus: true,
        autoselect: true,
        hint: false,
        keyboardShortcuts: [`s`],
      },
    });
  }

  componentWillUnmount() {
    window.removeEventListener(
      `autocomplete:selected`,
      this.autocompleteSelected,
      true
    );
  }

  onBlur = () => {
    this.setState({ open: false });
  };

  isVerticalScrollShown = () =>
    document.body.scrollHeight > document.body.clientHeight;

  render() {
    return (
      <div className={styles.searchBoxWrapper}>
        {this.state.open && (
          <Helmet
            bodyAttributes={{
              class: cx(styles.noscroll, {
                [styles.scrollDisabled]: this.isVerticalScrollShown(),
              }),
            }}
          />
        )}
        <div
          className={cx(styles.searchDimmer, {
            [styles.searchDimmerActive]: this.state.open,
          })}
          onClick={() => this.close()}
        />
        <div className={styles.searchBox}>
          <div
            className={cx(styles.searchBoxMagnifier, {
              [styles.searchBoxMagnifierActive]: this.state.open,
            })}
          />
          <input
            className={styles.searchBoxInput}
            ref={(ref) => (this.searchInput = ref)}
            type="search"
            id="search"
            placeholder="Search"
            aria-label="Search"
            onFocus={() => this.setState({ open: true })}
            onBlur={this.onBlur}
          />
        </div>
      </div>
    );
  }
  close(): void {
    throw new Error('Method not implemented.');
  }
}
