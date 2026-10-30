/**
 * Consent for analytics and advertising on the Cube docs.
 *
 * Mintlify runs every .js file in this folder on every page, so this file is the one place that starts trackers.
 * Nothing that identifies or follows a visitor (Google Tag Manager and the tags inside it, Clay, Cube's own page
 * tracking) loads until the visitor allows it:
 *
 * - Visitors whose browser time zone is outside the United States, or unknown, are asked first. Trackers stay off
 *   until they choose "Accept all".
 * - Visitors in a US time zone get the trackers and a notice with an opt-out.
 * - Global Privacy Control counts as an opt-out everywhere.
 *
 * The choice is kept for six months in the cube_consent cookie, scoped to .cube.dev so that other cube.dev sites can
 * share it. The "Cookie Preferences" link in the footer (docs.json) reopens the dialog.
 *
 * Do not start a tracker anywhere else (docs.json integrations, or another .js file here): it would run before the
 * visitor has chosen.
 */
(function () {
  if (window.__cubeConsent) return;
  window.__cubeConsent = true;

  var COOKIE = 'cube_consent';
  var MAX_AGE = 180 * 24 * 60 * 60;
  var GTM_ID = 'GTM-52W7VM2';
  var CLAY_SRC = 'https://static.claydar.com/init.v1.js?id=c2dQN6r6rp';
  var POLICY_URL = 'https://cube.dev/legal/privacy-policy';
  var OPEN_LINK = '#cookie-preferences';
  var SIGNALS = ['ad_storage', 'analytics_storage', 'ad_user_data', 'ad_personalization'];

  // United States time zones, including the territories IANA files under US/*.
  var US_TIME_ZONE = /^(America\/(New_York|Detroit|Chicago|Menominee|Denver|Boise|Phoenix|Los_Angeles|Anchorage|Juneau|Sitka|Metlakatla|Yakutat|Nome|Adak|Indiana\/\w+|Kentucky\/\w+|North_Dakota\/\w+)|Pacific\/Honolulu|US\/[\w-]+)$/;

  // Cookies the trackers set on the cube.dev domains; cleared when a visitor withdraws consent.
  var TRACKING_COOKIES = /^(_ga|_ga_.+|_gid|_gat.*|_gcl_.+|_gac_.+|__hs.+|hubspotutk|utm_params|_ce\..+|ceir|cebs.*|is_returning|_dd_s|__d_d_r.*|cubedev_anonymous|cube_id|unify_.+|swan.*|_fw.*|fw_.+|_reo.*|li_.+|bcookie|lidc|UserMatchHistory|AnalyticsSyncHistory|personalization_id|_clck|_clsk)$/;

  /* ===== Consent state ===== */

  function readCookie(name) {
    var parts = document.cookie.split(';');
    for (var i = 0; i < parts.length; i++) {
      var pair = parts[i].trim().split('=');
      if (pair[0] === name) return pair.slice(1).join('=');
    }
    return null;
  }

  function state() {
    var stored = readCookie(COOKIE);
    var timeZone;
    try {
      timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    } catch (e) {
      // An unknown time zone is treated like a non-US one.
    }
    var region = timeZone && US_TIME_ZONE.test(timeZone) ? 'opt-out' : 'opt-in';
    var gpc = navigator.globalPrivacyControl === true;
    var choice = stored === 'granted' || stored === 'denied' ? stored : null;
    return {
      region: region,
      choice: choice,
      gpc: gpc,
      status: choice || (gpc || region === 'opt-in' ? 'denied' : 'granted'),
      // A Global Privacy Control signal in a US time zone already is the visitor's answer.
      prompt: choice === null && !(gpc && region === 'opt-out'),
    };
  }

  function sharedDomain() {
    var host = location.hostname;
    return host === 'cube.dev' || /\.cube\.dev$/.test(host) ? '.cube.dev' : null;
  }

  function saveChoice(choice) {
    var cookie = COOKIE + '=' + choice + '; Path=/; Max-Age=' + MAX_AGE + '; SameSite=Lax';
    if (location.protocol === 'https:') cookie += '; Secure';
    var domain = sharedDomain();
    if (domain) {
      // Drop a host-only copy from an older version so it cannot shadow the shared one.
      document.cookie = COOKIE + '=; Path=/; Max-Age=0';
      cookie += '; Domain=' + domain;
    }
    document.cookie = cookie;
  }

  /* ===== Trackers ===== */

  function gtag() {
    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push(arguments);
  }

  function googleConsent(value) {
    var result = {};
    SIGNALS.forEach(function (signal) {
      result[signal] = value;
    });
    return result;
  }

  function loadScript(src) {
    var script = document.createElement('script');
    script.async = true;
    script.src = src;
    document.head.appendChild(script);
  }

  // Cube's own page tracking: an anonymous id shared across cube.dev, and a page event.
  function trackPage() {
    var COOKIE_NAME = 'cubedev_anonymous';
    var ID_MAX_AGE = 365 * 24 * 60 * 60;
    var TRACK_URL = 'https://track.cube.dev/track';
    var IDENTITY_URL = 'https://identity.cube.dev';

    function getCookie(name) {
      var match = document.cookie.match(new RegExp('(?:^|; )' + name + '=([^;]*)'));
      return match ? decodeURIComponent(match[1]) : null;
    }

    function setCookie(name, value, maxAge, domain) {
      var cookie = encodeURIComponent(name) + '=' + encodeURIComponent(value) +
        '; max-age=' + maxAge + '; path=/; SameSite=Lax';
      if (domain) cookie += '; domain=' + domain;
      document.cookie = cookie;
    }

    function getTopDomain() {
      var parts = location.hostname.split('.');
      if (parts.length <= 1) return location.hostname;
      for (var i = parts.length - 2; i >= 0; i--) {
        var candidate = parts.slice(i).join('.');
        setCookie('__tld__', '1', 10, '.' + candidate);
        if (getCookie('__tld__')) {
          setCookie('__tld__', '', -1, '.' + candidate);
          return candidate;
        }
      }
      return location.hostname;
    }

    function newUUID() {
      if (typeof crypto !== 'undefined' && crypto.randomUUID) {
        return crypto.randomUUID();
      }
      return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        var r = (Math.random() * 16) | 0;
        return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
      });
    }

    function getAnonymousId(domain, callback) {
      var existing = getCookie(COOKIE_NAME);
      if (existing) return callback(existing);

      fetch(IDENTITY_URL, { credentials: 'include' })
        .then(function (r) { return r.status < 400 ? r.text() : Promise.reject(); })
        .catch(function () { return newUUID(); })
        .then(function (id) {
          setCookie(COOKIE_NAME, id, ID_MAX_AGE, domain ? '.' + domain : null);
          callback(id);
        });
    }

    getAnonymousId(getTopDomain(), function (anonymousId) {
      var payload = [{
        event: 'page',
        href: location.href,
        pathname: location.pathname,
        search: location.search,
        hash: location.hash,
        referrer: document.referrer,
        id: newUUID(),
        clientAnonymousId: anonymousId,
        clientTimestamp: new Date().toJSON(),
        sentAt: new Date().toJSON(),
      }];

      fetch(TRACK_URL, {
        method: 'POST',
        body: JSON.stringify(payload),
        headers: { 'Content-Type': 'application/json' },
      }).catch(function () {});
    });
  }

  var trackersLoaded = false;

  function startTrackers() {
    gtag('consent', 'update', googleConsent('granted'));
    if (trackersLoaded) return;
    trackersLoaded = true;

    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push({ 'gtm.start': new Date().getTime(), event: 'gtm.js' });
    loadScript('https://www.googletagmanager.com/gtm.js?id=' + GTM_ID);
    loadScript(CLAY_SRC);
    trackPage();
  }

  function isTrackingCookie(name) {
    if (TRACKING_COOKIES.test(name)) return true;
    // Unify stores its ids under a base64 encoding of "<api key>_unify_*_id".
    try {
      return /_unify_(session|visitor)_id/.test(atob(name));
    } catch (e) {
      return false;
    }
  }

  function clearTrackingCookies() {
    var labels = location.hostname.split('.');
    var domains = [];
    for (var i = 0; i < labels.length - 1; i++) domains.push(labels.slice(i).join('.'));

    document.cookie.split(';').forEach(function (cookie) {
      var name = cookie.split('=')[0].trim();
      if (!isTrackingCookie(name)) return;

      var expired = name + '=; Max-Age=0; Path=/';
      document.cookie = expired;
      domains.forEach(function (domain) {
        document.cookie = expired + '; Domain=' + domain;
        document.cookie = expired + '; Domain=.' + domain;
      });
    });
  }

  /* ===== Dialog ===== */

  var dialog = null;

  function closeDialog() {
    if (dialog && dialog.parentNode) dialog.parentNode.removeChild(dialog);
    dialog = null;
  }

  function element(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text) node.textContent = text;
    return node;
  }

  function choose(choice) {
    var previous = state().status;
    saveChoice(choice);
    closeDialog();

    if (choice === 'granted' && previous !== 'granted') startTrackers();

    if (choice === 'denied' && previous === 'granted') {
      // Scripts that already ran cannot be unloaded; a reload drops them.
      gtag('consent', 'update', googleConsent('denied'));
      clearTrackingCookies();
      location.reload();
    }
  }

  function openDialog() {
    closeDialog();

    var current = state();
    var optIn = current.region === 'opt-in';
    var asking = current.prompt;

    dialog = element('section', 'cube-consent');
    dialog.setAttribute('role', 'dialog');
    dialog.setAttribute('aria-modal', 'false');
    dialog.setAttribute('aria-labelledby', 'consent-title');
    dialog.setAttribute('aria-describedby', 'consent-description');

    if (!asking) {
      var close = element('button', 'cube-consent__close', '\u00d7');
      close.type = 'button';
      close.setAttribute('aria-label', 'Close');
      close.addEventListener('click', closeDialog);
      dialog.appendChild(close);
    }

    var title = element('h2', 'cube-consent__title', optIn && asking ? 'Cookies on cube.dev' : 'Cookie preferences');
    title.id = 'consent-title';
    dialog.appendChild(title);

    var text = element('p', 'cube-consent__text', optIn
      ? 'We use analytics and advertising cookies to understand how people use our site and to measure our ads. They stay off unless you accept. '
      : 'We use analytics and advertising cookies. Under some US state privacy laws, that can count as selling or sharing personal information. You can opt out here, or later with the Cookie Preferences link in the footer. ');
    text.id = 'consent-description';
    var link = element('a', 'cube-consent__link', 'Privacy Policy');
    link.href = POLICY_URL;
    text.appendChild(link);
    dialog.appendChild(text);

    if (!asking) {
      var status = current.status === 'granted' ? 'Analytics and advertising cookies are on.' : 'Analytics and advertising cookies are off.';
      if (current.gpc && current.choice === null) status += ' Your browser sends Global Privacy Control, which we treat as an opt-out.';
      dialog.appendChild(element('p', 'cube-consent__status', status));
    }

    var actions = element('div', 'cube-consent__actions');
    var reject = element('button', 'cube-consent__button', optIn ? 'Reject non-essential' : 'Opt out');
    reject.type = 'button';
    reject.addEventListener('click', function () { choose('denied'); });
    var accept = element('button', 'cube-consent__button', optIn ? 'Accept all' : 'Accept');
    accept.type = 'button';
    accept.addEventListener('click', function () { choose('granted'); });
    actions.appendChild(reject);
    actions.appendChild(accept);
    dialog.appendChild(actions);

    document.body.appendChild(dialog);
    if (!asking) accept.focus();
  }

  /* ===== Start ===== */

  function start() {
    gtag('consent', 'default', googleConsent('denied'));

    var current = state();
    if (current.status === 'granted') startTrackers();
    if (current.prompt) openDialog();

    // The footer link. Capture phase, so Mintlify's router does not treat it as page navigation.
    document.addEventListener('click', function (event) {
      var anchor = event.target && event.target.closest ? event.target.closest('a[href$="' + OPEN_LINK + '"]') : null;
      if (!anchor) return;
      event.preventDefault();
      event.stopPropagation();
      openDialog();
    }, true);

    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape' && dialog && !state().prompt) closeDialog();
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
