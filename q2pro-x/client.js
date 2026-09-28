// q2pro-x/client.js
//
// Client-side JS served inline in the page. Three small self-contained
// features, no external deps:
//   1. Language switcher — toggles between RU/EN via querystring +
//      localStorage, so a click re-navigates to ?lang=xx and the next
//      page view remembers the choice.
//   2. Screenshot lightbox — click any .qpx-shot to open the full image
//      (or a placeholder if the file does not yet exist). Close via X,
//      backdrop click, or Esc.
//   3. Document preview modal — click any docs card to open an iframe
//      preview page, with a separate Download button in the modal.
//   4. Mobile nav — the header toggle opens/closes the section menu
//      on narrow viewports, and clicking any nav link auto-closes it.
//
// Exports the client JS as a string (injected into <script> in render.js).

const JS = `
(function () {
  'use strict';

  function syncBodyLock() {
    var anyOpen = !!document.querySelector('.qpx-modal.is-open');
    document.body.classList.toggle('qpx-modal-locked', anyOpen);
  }

  function requestFullscreenCompat(el) {
    if (!el) return Promise.resolve();
    var fn = el.requestFullscreen ||
             el.webkitRequestFullscreen ||
             el.msRequestFullscreen;
    if (!fn) return Promise.resolve();
    try {
      var res = fn.call(el);
      return res && typeof res.then === 'function' ? res : Promise.resolve();
    } catch (e) {
      return Promise.resolve();
    }
  }

  function exitFullscreenCompat() {
    var fn = document.exitFullscreen ||
             document.webkitExitFullscreen ||
             document.msExitFullscreen;
    if (!fn) return Promise.resolve();
    try {
      var res = fn.call(document);
      return res && typeof res.then === 'function' ? res : Promise.resolve();
    } catch (e) {
      return Promise.resolve();
    }
  }

  function isFullscreenActive() {
    return !!(document.fullscreenElement || document.webkitFullscreenElement || document.msFullscreenElement);
  }

  function toggleFullscreen(el) {
    return isFullscreenActive() ? exitFullscreenCompat() : requestFullscreenCompat(el);
  }

  /* ─────────── LANG SWITCH ─────────── */

  var LANG_KEY = 'qpx_lang';
  var THEME_COOKIE = 'qpx_theme';

  function setLang(lang) {
    try { localStorage.setItem(LANG_KEY, lang); } catch (e) {}
    var url = new URL(window.location.href);
    url.searchParams.set('lang', lang);
    window.location.href = url.toString();
  }

  document.querySelectorAll('[data-qpx-lang]').forEach(function (btn) {
    btn.addEventListener('click', function (ev) {
      ev.preventDefault();
      var lang = btn.getAttribute('data-qpx-lang');
      if (lang) setLang(lang);
    });
  });

  /* ─────────── THEME SWITCH ─────────── */

  var root = document.documentElement;
  var themeSelects = Array.prototype.slice.call(document.querySelectorAll('[data-qpx-theme-select]'));
  var themeMq = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;

  function setCookie(name, value, maxAgeSeconds) {
    var cookie = name + '=' + encodeURIComponent(value) + '; Path=/; SameSite=Lax';
    if (maxAgeSeconds) cookie += '; Max-Age=' + String(maxAgeSeconds);
    document.cookie = cookie;
  }

  function resolveThemeMode(mode) {
    mode = String(mode || '').toLowerCase();
    return (mode === 'dark' || mode === 'light' || mode === 'auto') ? mode : 'auto';
  }

  function resolveThemeActual(mode) {
    mode = resolveThemeMode(mode);
    if (mode === 'dark' || mode === 'light') return mode;
    return themeMq && themeMq.matches ? 'dark' : 'light';
  }

  function applyTheme(mode, persist) {
    mode = resolveThemeMode(mode);
    var actual = resolveThemeActual(mode);
    root.setAttribute('data-qpx-theme-mode', mode);
    root.setAttribute('data-qpx-theme', actual);
    var meta = document.querySelector('meta[name="theme-color"]');
    if (meta) meta.setAttribute('content', actual === 'light' ? '#f4f7fc' : '#0a0d15');
    themeSelects.forEach(function (sel) { sel.value = mode; });
    if (persist) setCookie(THEME_COOKIE, mode, 31536000);
  }

  themeSelects.forEach(function (sel) {
    sel.addEventListener('change', function () {
      applyTheme(sel.value, true);
    });
  });

  if (themeMq) {
    var onThemeChange = function () {
      if (resolveThemeMode(root.getAttribute('data-qpx-theme-mode')) === 'auto') {
        applyTheme('auto', false);
      }
    };
    if (themeMq.addEventListener) themeMq.addEventListener('change', onThemeChange);
    else if (themeMq.addListener) themeMq.addListener(onThemeChange);
  }

  applyTheme(root.getAttribute('data-qpx-theme-mode'), false);

  /* ─────────── MOBILE NAV TOGGLE ─────────── */

  var navToggle = document.querySelector('[data-qpx-nav-toggle]');
  var nav       = document.querySelector('[data-qpx-nav]');
  if (navToggle && nav) {
    navToggle.addEventListener('click', function () {
      var open = nav.classList.toggle('is-open');
      navToggle.setAttribute('aria-expanded', open ? 'true' : 'false');
    });
    nav.querySelectorAll('a, button').forEach(function (link) {
      link.addEventListener('click', function () {
        nav.classList.remove('is-open');
        navToggle.setAttribute('aria-expanded', 'false');
      });
    });
  }

  /* ─────────── LIGHTBOX / MODAL ─────────── */

  var modal       = document.querySelector('[data-qpx-modal]');
  var modalImg    = document.querySelector('[data-qpx-modal-img]');
  var modalCap    = document.querySelector('[data-qpx-modal-caption]');
  var modalFrame  = document.querySelector('[data-qpx-modal-frame]');
  var modalClose  = document.querySelector('[data-qpx-modal-close]');
  var modalFs     = document.querySelector('[data-qpx-modal-fullscreen]');
  var modalPrev   = document.querySelector('[data-qpx-modal-prev]');
  var modalNext   = document.querySelector('[data-qpx-modal-next]');
  var placeholderText = modal ? modal.getAttribute('data-placeholder-text') || 'no image yet' : 'no image yet';
  var currentGallery = [];
  var currentIndex = -1;

  function setModalNavState() {
    var hasPrev = currentGallery.length > 1 && currentIndex > 0;
    var hasNext = currentGallery.length > 1 && currentIndex >= 0 && currentIndex < currentGallery.length - 1;
    if (modalPrev) {
      modalPrev.hidden = !hasPrev;
      modalPrev.disabled = !hasPrev;
    }
    if (modalNext) {
      modalNext.hidden = !hasNext;
      modalNext.disabled = !hasNext;
    }
  }

  function resolveGalleryFromTile(tile) {
    if (!tile) return [tile].filter(Boolean);
    var scope = tile.closest('.qpx-images');
    if (!scope) return [tile];
    return Array.prototype.slice.call(scope.querySelectorAll('[data-qpx-shot]'));
  }

  function openModalTile(tile) {
    if (!tile || !modal) return;
    currentGallery = resolveGalleryFromTile(tile);
    currentIndex = Math.max(0, currentGallery.indexOf(tile));
    setModalNavState();

    var src = tile.getAttribute('data-src') || '';
    var caption = tile.getAttribute('data-caption') || '';
    var alt = tile.getAttribute('data-alt') || '';

    if (!modal) return;
    modal.classList.add('is-open');
    syncBodyLock();
    modalCap.textContent = caption || '';
    modalImg.alt = alt || '';

    // Try to load the image; if it 404s, show a placeholder card instead.
    modalImg.style.display = 'block';
    modalFrame.classList.remove('qpx-modal__frame--placeholder');
    var ph = modalFrame.querySelector('.qpx-modal__placeholder');
    if (ph) ph.remove();

    modalImg.onerror = function () {
      modalImg.style.display = 'none';
      var node = document.createElement('div');
      node.className = 'qpx-modal__placeholder';
      node.textContent = placeholderText + ': ' + src.replace(/^.+\\//, '');
      modalFrame.appendChild(node);
    };
    modalImg.src = src;

    // Trap focus on the close button so Esc / Enter work immediately.
    if (modalClose) modalClose.focus();
  }

  function closeModal() {
    if (!modal) return;
    modal.classList.remove('is-open');
    syncBodyLock();
    modalImg.removeAttribute('src');
    modalCap.textContent = '';
    currentGallery = [];
    currentIndex = -1;
    setModalNavState();
  }

  function stepModal(delta) {
    if (!currentGallery.length) return;
    var nextIndex = currentIndex + delta;
    if (nextIndex < 0 || nextIndex >= currentGallery.length) return;
    openModalTile(currentGallery[nextIndex]);
  }

  document.querySelectorAll('[data-qpx-shot]').forEach(function (tile) {
    tile.addEventListener('click', function () {
      openModalTile(tile);
    });
  });

  if (modal) {
    modal.addEventListener('click', function (ev) {
      // click outside inner frame closes the modal
      if (ev.target === modal) closeModal();
    });
  }
  if (modalClose) modalClose.addEventListener('click', closeModal);
  if (modalFs) {
    modalFs.addEventListener('click', function () {
      toggleFullscreen(modalFrame);
    });
  }
  if (modalPrev) {
    modalPrev.addEventListener('click', function (ev) {
      ev.stopPropagation();
      stepModal(-1);
    });
  }
  if (modalNext) {
    modalNext.addEventListener('click', function (ev) {
      ev.stopPropagation();
      stepModal(1);
    });
  }

  /* ─────────── DOC PREVIEW MODAL ─────────── */

  var docModal      = document.querySelector('[data-qpx-doc-modal]');
  var docClose      = document.querySelector('[data-qpx-doc-close]');
  var docTitle      = document.querySelector('[data-qpx-doc-title]');
  var docIframe     = document.querySelector('[data-qpx-doc-iframe]');
  var docDownload   = document.querySelector('[data-qpx-doc-download]');
  var docFs         = document.querySelector('[data-qpx-doc-fullscreen]');
  var docFrame      = docModal ? docModal.querySelector('.qpx-doc-modal__frame') : null;

  function openDocModal(preview, download, title) {
    if (!docModal || !docIframe) return;
    if (docTitle) docTitle.textContent = title || '';
    if (docDownload) {
      docDownload.href = download || preview || '#';
    }
    docIframe.src = preview || 'about:blank';
    docModal.classList.add('is-open');
    syncBodyLock();
    if (docClose) docClose.focus();
  }

  function closeDocModal() {
    if (!docModal) return;
    docModal.classList.remove('is-open');
    if (docIframe) docIframe.src = 'about:blank';
    if (docTitle) docTitle.textContent = '';
    if (docDownload) docDownload.href = '#';
    syncBodyLock();
  }

  document.querySelectorAll('[data-qpx-doc-card]').forEach(function (card) {
    card.addEventListener('click', function (ev) {
      ev.preventDefault();
      var preview = card.getAttribute('data-preview') || card.getAttribute('href') || '';
      var download = card.getAttribute('data-download') || preview;
      var title = card.getAttribute('data-title') || '';
      openDocModal(preview, download, title);
    });
  });

  if (docModal) {
    docModal.addEventListener('click', function (ev) {
      if (ev.target === docModal) closeDocModal();
    });
  }
  if (docClose) docClose.addEventListener('click', closeDocModal);
  if (docFs) {
    docFs.addEventListener('click', function () {
      toggleFullscreen(docFrame);
    });
  }

  /* ─────────── RELEASE STATUS MODAL ─────────── */

  var releaseModal  = document.querySelector('[data-qpx-release-modal]');
  var releaseClose  = document.querySelector('[data-qpx-release-close]');
  var releaseTitle  = document.querySelector('[data-qpx-release-title]');
  var releaseText   = document.querySelector('[data-qpx-release-text]');
  var releaseDefaultTitle = releaseTitle ? releaseTitle.textContent : '';
  var releaseDefaultText = releaseText ? releaseText.textContent : '';

  function openReleaseModal(trigger) {
    if (!releaseModal) return;
    var title = trigger ? (trigger.getAttribute('data-qpx-release-title') || '') : '';
    var text = trigger ? (trigger.getAttribute('data-qpx-release-message') || '') : '';
    if (releaseTitle) releaseTitle.textContent = title || releaseDefaultTitle;
    if (releaseText) releaseText.textContent = text || releaseDefaultText;
    releaseModal.classList.add('is-open');
    syncBodyLock();
    if (releaseClose) releaseClose.focus();
  }

  function closeReleaseModal() {
    if (!releaseModal) return;
    releaseModal.classList.remove('is-open');
    if (releaseTitle) releaseTitle.textContent = releaseDefaultTitle;
    if (releaseText) releaseText.textContent = releaseDefaultText;
    syncBodyLock();
  }

  document.querySelectorAll('[data-qpx-release-open]').forEach(function (btn) {
    btn.addEventListener('click', function (ev) {
      ev.preventDefault();
      openReleaseModal(btn);
    });
  });

  if (releaseModal) {
    releaseModal.addEventListener('click', function (ev) {
      if (ev.target === releaseModal) closeReleaseModal();
    });
  }
  if (releaseClose) releaseClose.addEventListener('click', closeReleaseModal);

  document.addEventListener('keydown', function (ev) {
    if (releaseModal && releaseModal.classList.contains('is-open') &&
        (ev.key === 'Escape' || ev.key === 'Esc')) {
      ev.preventDefault();
      closeReleaseModal();
      return;
    }
    if (docModal && docModal.classList.contains('is-open') &&
        (ev.key === 'Escape' || ev.key === 'Esc')) {
      ev.preventDefault();
      closeDocModal();
      return;
    }
    if (modal && modal.classList.contains('is-open') &&
        (ev.key === 'Escape' || ev.key === 'Esc')) {
      ev.preventDefault();
      closeModal();
      return;
    }
    if (modal && modal.classList.contains('is-open') &&
        (ev.key === 'ArrowLeft' || ev.key === 'Left')) {
      ev.preventDefault();
      stepModal(-1);
      return;
    }
    if (modal && modal.classList.contains('is-open') &&
        (ev.key === 'ArrowRight' || ev.key === 'Right')) {
      ev.preventDefault();
      stepModal(1);
    }
  });
})();
`;

module.exports = { JS };
