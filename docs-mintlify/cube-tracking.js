(function () {
  var COOKIE_NAME = 'cubedev_anonymous';
  var MAX_AGE = 365 * 24 * 60 * 60;
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
        setCookie(COOKIE_NAME, id, MAX_AGE, domain ? '.' + domain : null);
        callback(id);
      });
  }

  function trackPage() {
    var topDomain = getTopDomain();
    getAnonymousId(topDomain, function (anonymousId) {
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

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', trackPage);
  } else {
    trackPage();
  }
})();
