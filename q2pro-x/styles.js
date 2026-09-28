// q2pro-x/styles.js
//
// CSS for the /q2pro-x page. Exported as a string and injected into
// <style> in the page shell. No external stylesheets, no framework.
//
// Visual direction (per TZ): sci-fi / Quake II / smart-client identity.
// Dark palette, narrow "industrial" accents, monospace for codey bits.
// Light-touch motion (hover glows, no keyframe blobs).

const CSS = `
/* ─────────── RESET / BASE ─────────── */

:root {
  --qpx-bg-top: #05070d;
  --qpx-bg-mid: #0a0d15;
  --qpx-bg-bottom: #05070d;
  --qpx-bg-spot-a: rgba(120,180,255,0.08);
  --qpx-bg-spot-b: rgba(200,80,220,0.06);
  --qpx-text: #d7dbe3;
  --qpx-text-strong: #eaf1ff;
  --qpx-text-muted: #aab4c6;
  --qpx-text-soft: #9aa6bc;
  --qpx-text-faint: #8fa0c0;
  --qpx-link: #9cc3ff;
  --qpx-link-hover: #cfe2ff;
  --qpx-accent: #6ea9ff;
  --qpx-accent-strong: #38b4ff;
  --qpx-accent-alt: #b45cff;
  --qpx-header-bg: rgba(8, 10, 18, 0.85);
  --qpx-header-border: rgba(120, 150, 210, 0.15);
  --qpx-border-soft: rgba(120, 150, 210, 0.15);
  --qpx-border-mid: rgba(120, 150, 210, 0.2);
  --qpx-border-strong: rgba(120, 150, 210, 0.25);
  --qpx-border-accent: rgba(110, 169, 255, 0.35);
  --qpx-surface-0: rgba(255, 255, 255, 0.02);
  --qpx-surface-1: rgba(255, 255, 255, 0.04);
  --qpx-surface-2: rgba(255, 255, 255, 0.08);
  --qpx-surface-inset: rgba(0, 0, 0, 0.15);
  --qpx-surface-accent: rgba(110, 169, 255, 0.06);
  --qpx-shot-bg: #0c101a;
  --qpx-modal-backdrop: rgba(0, 0, 0, 0.85);
  --qpx-modal-bg: rgba(10, 13, 21, 0.92);
  --qpx-modal-utility-bg: rgba(10, 13, 21, 0.9);
  --qpx-shadow: rgba(0, 0, 0, 0.3);
  --qpx-theme-select-bg: rgba(255, 255, 255, 0.02);
}

html[data-qpx-theme="light"] {
  --qpx-bg-top: #f4f7fc;
  --qpx-bg-mid: #eef3fb;
  --qpx-bg-bottom: #f7faff;
  --qpx-bg-spot-a: rgba(67, 126, 255, 0.12);
  --qpx-bg-spot-b: rgba(159, 90, 222, 0.08);
  --qpx-text: #22314c;
  --qpx-text-strong: #101b2d;
  --qpx-text-muted: #485a79;
  --qpx-text-soft: #5d6f8d;
  --qpx-text-faint: #617392;
  --qpx-link: #295bdc;
  --qpx-link-hover: #173a91;
  --qpx-accent: #295bdc;
  --qpx-accent-strong: #2f8fff;
  --qpx-accent-alt: #8a3dd1;
  --qpx-header-bg: rgba(248, 251, 255, 0.9);
  --qpx-header-border: rgba(52, 73, 112, 0.14);
  --qpx-border-soft: rgba(52, 73, 112, 0.14);
  --qpx-border-mid: rgba(52, 73, 112, 0.18);
  --qpx-border-strong: rgba(52, 73, 112, 0.22);
  --qpx-border-accent: rgba(41, 91, 220, 0.32);
  --qpx-surface-0: rgba(255, 255, 255, 0.72);
  --qpx-surface-1: rgba(255, 255, 255, 0.88);
  --qpx-surface-2: rgba(255, 255, 255, 0.96);
  --qpx-surface-inset: rgba(209, 220, 239, 0.35);
  --qpx-surface-accent: rgba(41, 91, 220, 0.08);
  --qpx-shot-bg: #e9eef8;
  --qpx-modal-backdrop: rgba(16, 24, 40, 0.55);
  --qpx-modal-bg: rgba(250, 252, 255, 0.97);
  --qpx-modal-utility-bg: rgba(255, 255, 255, 0.96);
  --qpx-shadow: rgba(42, 64, 105, 0.16);
  --qpx-theme-select-bg: rgba(255, 255, 255, 0.92);
}

*,*::before,*::after { box-sizing: border-box; }
html, body { margin: 0; padding: 0; }
html { scroll-behavior: smooth; scroll-padding-top: 72px; }

body {
  font-family: 'Inter', 'Segoe UI', Roboto, -apple-system, BlinkMacSystemFont,
               'Helvetica Neue', Arial, sans-serif;
  font-size: 16px;
  line-height: 1.55;
  color: var(--qpx-text);
  background:
    radial-gradient(1200px 600px at 20% -10%, var(--qpx-bg-spot-a), transparent 60%),
    radial-gradient(900px 500px at 95% 10%,  var(--qpx-bg-spot-b),  transparent 55%),
    linear-gradient(180deg, var(--qpx-bg-top) 0%, var(--qpx-bg-mid) 50%, var(--qpx-bg-bottom) 100%);
  min-height: 100vh;
  overflow-x: hidden;
}

.qpx-wrap { max-width: 1200px; margin: 0 auto; padding: 0 24px; }

img { max-width: 100%; display: block; }
a { color: var(--qpx-link); text-decoration: none; transition: color .15s ease; }
a:hover, a:focus { color: var(--qpx-link-hover); }
a:focus-visible { outline: 2px solid var(--qpx-accent); outline-offset: 2px; border-radius: 3px; }

button {
  font-family: inherit; font-size: inherit; color: inherit;
  background: transparent; border: 0; cursor: pointer;
}
button:focus-visible { outline: 2px solid var(--qpx-accent); outline-offset: 2px; }

code, pre, .qpx-mono {
  font-family: 'JetBrains Mono', 'Fira Code', Consolas, 'Liberation Mono', monospace;
  font-size: 0.92em;
}

/* ─────────── SKIP LINK (accessibility) ─────────── */

.qpx-skip {
  position: absolute; left: -9999px; top: 12px;
  background: var(--qpx-bg-mid); color: var(--qpx-link-hover);
  padding: 8px 14px; border-radius: 4px;
}
.qpx-skip:focus { left: 12px; z-index: 1000; }

/* ─────────── HEADER ─────────── */

.qpx-header {
  position: sticky; top: 0; z-index: 40;
  background: var(--qpx-header-bg);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  border-bottom: 1px solid var(--qpx-header-border);
}
.qpx-header__inner {
  position: relative;
  display: grid;
  gap: 10px;
  padding-top: 10px;
  padding-bottom: 10px;
}
.qpx-header__top {
  display: flex;
  align-items: center;
  gap: 16px;
  min-height: 42px;
}
.qpx-brand {
  display: flex; align-items: center; gap: 10px;
  font-weight: 700; font-size: 18px; letter-spacing: 0.08em;
  color: var(--qpx-text-strong); text-transform: uppercase; flex-shrink: 0;
  min-width: 0;
}
.qpx-brand__mark {
  width: 32px; height: 32px; border-radius: 6px;
  background: linear-gradient(135deg, var(--qpx-accent-strong) 0%, var(--qpx-accent-alt) 100%);
  display: inline-flex; align-items: center; justify-content: center;
  color: #0a0d15; font-weight: 900; font-size: 14px;
  box-shadow: 0 0 20px rgba(56, 180, 255, 0.35);
  font-family: 'JetBrains Mono', Consolas, monospace;
}
.qpx-brand__tag {
  font-size: 11px; font-weight: 400;
  color: var(--qpx-text-faint); text-transform: uppercase; letter-spacing: 0.15em;
  margin-left: 4px;
}

.qpx-nav {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  justify-content: center;
  gap: 4px;
  overflow: visible;
  scrollbar-width: none;
  white-space: normal;
}
.qpx-nav::-webkit-scrollbar { display: none; }
.qpx-nav__item {
  padding: 7px 8px; border-radius: 4px;
  color: var(--qpx-text-muted); font-size: 12px; font-weight: 500;
  letter-spacing: 0.02em; white-space: nowrap;
  transition: all .15s ease;
}
.qpx-nav__item:hover, .qpx-nav__item:focus-visible {
  color: var(--qpx-text-strong); background: var(--qpx-surface-2);
}
.qpx-nav__item--aux { display: none; }
.qpx-nav__item--release {
  border: 1px solid var(--qpx-border-mid);
  background: linear-gradient(135deg, rgba(56,180,255,0.14), rgba(180,92,255,0.10));
}
.qpx-nav__item--button {
  border: 0;
  background: transparent;
  text-align: left;
  cursor: pointer;
}
.qpx-nav__icon {
  display: inline-block; margin-right: 4px;
  color: var(--qpx-accent); font-weight: 700;
}

.qpx-header__right {
  display: flex; align-items: center; gap: 8px;
  flex-shrink: 0;
  flex-wrap: wrap;
  justify-content: flex-end;
  margin-left: auto;
}

.qpx-theme {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 36px;
  padding: 0 10px;
  border: 1px solid var(--qpx-border-mid);
  border-radius: 4px;
  background: var(--qpx-theme-select-bg);
}
.qpx-theme__label {
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--qpx-text-faint);
}
.qpx-theme__select {
  min-width: 86px;
  border: 0;
  background: transparent;
  color: var(--qpx-text-strong);
  font-size: 13px;
  font-weight: 600;
  outline: none;
}
.qpx-theme__select option {
  color: #101b2d;
}

.qpx-lang {
  display: inline-flex; border: 1px solid var(--qpx-border-mid);
  border-radius: 4px; overflow: hidden;
}
.qpx-lang__btn {
  padding: 6px 10px; font-size: 12px; font-weight: 600;
  letter-spacing: 0.08em; color: var(--qpx-text-faint);
  transition: all .15s ease;
}
.qpx-lang__btn:hover { color: var(--qpx-text-strong); background: var(--qpx-surface-2); }
.qpx-lang__btn[aria-pressed="true"] {
  background: linear-gradient(135deg, var(--qpx-accent-strong) 0%, var(--qpx-accent) 100%);
  color: #0a0d15;
}

.qpx-docs-cta {
  padding: 8px 14px; border: 1px solid var(--qpx-border-strong);
  border-radius: 4px; font-size: 13px; font-weight: 600;
  color: var(--qpx-link-hover); transition: all .15s ease;
}
.qpx-docs-cta:hover { border-color: var(--qpx-accent); background: rgba(110, 169, 255, 0.1); }
.qpx-docs-cta--release {
  background: linear-gradient(135deg, rgba(56,180,255,0.18), rgba(180,92,255,0.14));
}
.qpx-social {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 36px;
  padding: 0 12px;
  border: 1px solid var(--qpx-border-strong);
  border-radius: 4px;
  color: var(--qpx-link-hover);
  background: var(--qpx-surface-0);
}
.qpx-social:hover,
.qpx-social:focus-visible {
  border-color: var(--qpx-accent);
  background: rgba(110,169,255,0.10);
}
.qpx-social__icon {
  display: inline-flex;
  width: 16px;
  height: 16px;
}
.qpx-social__icon svg {
  width: 16px;
  height: 16px;
  fill: currentColor;
}
.qpx-social__label {
  font-size: 13px;
  font-weight: 600;
}

/* Mobile menu toggle (<=820px) */
.qpx-nav-toggle {
  display: none;
  padding: 8px; border: 1px solid var(--qpx-border-mid);
  border-radius: 4px; color: var(--qpx-link-hover);
}

/* ─────────── HERO ─────────── */

.qpx-hero {
  padding: 14px 0 30px;
  border-bottom: 1px solid var(--qpx-border-soft);
  position: relative; overflow: hidden;
}
.qpx-hero::before {
  content: ''; position: absolute;
  top: 50%; left: 50%; width: 700px; height: 700px;
  transform: translate(-50%, -50%);
  background: radial-gradient(circle, rgba(56, 180, 255, 0.06) 0%, transparent 60%);
  pointer-events: none;
}
.qpx-hero__inner { position: relative; z-index: 1; }
.qpx-hero__logo-wrap {
  margin: 0 0 6px;
  display: block;
  width: 100%;
  max-width: 100%;
  min-width: 0;
  overflow: hidden;
}
.qpx-hero__logo {
  display: block;
  width: 100%;
  max-width: 760px;
  min-width: 0;
  flex: 0 1 auto;
  height: auto;
  max-height: 330px;
  object-fit: contain;
  margin: 0 auto;
  filter: drop-shadow(0 10px 26px rgba(38, 84, 170, 0.14));
}
.qpx-hero__logo--light {
  display: none;
}
html[data-qpx-theme="light"] .qpx-hero__logo--dark {
  display: none;
}
html[data-qpx-theme="light"] .qpx-hero__logo--light {
  display: block;
}
.qpx-hero__meta-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  align-items: center;
  gap: 18px;
  width: 100%;
  margin: 0 0 12px;
}
.qpx-hero__eyebrow {
  font-family: 'JetBrains Mono', Consolas, monospace;
  display: inline-flex;
  align-items: center;
  padding: 7px 14px;
  border: 1px solid var(--qpx-border-strong);
  border-radius: 999px;
  background: var(--qpx-surface-1);
  font-size: 14px;
  font-weight: 800;
  letter-spacing: 0.22em;
  color: var(--qpx-accent);
  justify-self: start;
  margin: 0;
}
.qpx-hero__title {
  font-size: clamp(32px, 5vw, 56px); font-weight: 800;
  line-height: 1.05; margin: 0 0 16px;
  background: linear-gradient(135deg, var(--qpx-text-strong) 0%, var(--qpx-link) 100%);
  -webkit-background-clip: text; background-clip: text;
  -webkit-text-fill-color: transparent;
  letter-spacing: 0;
}
.qpx-hero__subtitle {
  font-size: clamp(15px, 1.4vw, 18px);
  color: var(--qpx-text-muted); margin: 0 0 18px;
  max-width: 1160px; line-height: 1.6;
}
.qpx-hero__release-note {
  margin: 0 0 16px;
  max-width: 1160px;
  color: var(--qpx-text-soft);
}
.qpx-hero__birth {
  display: inline-grid;
  grid-template-columns: auto auto auto;
  align-items: center;
  gap: 8px 10px;
  justify-self: center;
  margin: 0;
  padding: 9px 13px;
  border: 1px solid var(--qpx-border-soft);
  border-radius: 6px;
  background: var(--qpx-surface-0);
  color: var(--qpx-text-soft);
}
.qpx-hero__birth-icon {
  width: 22px; height: 22px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
  background: var(--qpx-accent);
  color: #05070d;
  font-size: 13px;
  font-weight: 900;
}
.qpx-hero__birth-label {
  font-size: 13px;
  font-weight: 800;
  color: var(--qpx-text-strong);
}
.qpx-hero__birth-value {
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 13px;
  color: var(--qpx-link-hover);
}
.qpx-hero__actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin: 0 0 18px;
}
.qpx-hero__pillars {
  display: grid; gap: 14px;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
}
.qpx-pillar {
  padding: 16px 18px; border: 1px solid var(--qpx-border-soft);
  border-radius: 6px; background: var(--qpx-surface-0);
  transition: all .2s ease;
}
.qpx-pillar:hover {
  border-color: var(--qpx-border-accent);
  background: rgba(110, 169, 255, 0.04);
  transform: translateY(-2px);
}
.qpx-pillar__title {
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 13px; font-weight: 700;
  color: var(--qpx-text-strong); letter-spacing: 0.06em;
  margin: 0 0 6px;
}
.qpx-pillar__desc { font-size: 14px; color: var(--qpx-text-soft); margin: 0; line-height: 1.5; }

/* ─────────── SECTION ─────────── */

.qpx-section { padding: 64px 0; scroll-margin-top: 72px; }
.qpx-section + .qpx-section { border-top: 1px solid var(--qpx-border-soft); }

.qpx-section__head { margin-bottom: 32px; }
.qpx-section__id {
  display: inline-flex;
  align-items: center;
  padding: 6px 12px;
  border: 1px solid var(--qpx-border-strong);
  border-radius: 999px;
  background: var(--qpx-surface-1);
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 14px;
  font-weight: 800;
  color: var(--qpx-accent);
  letter-spacing: 0.16em; margin-bottom: 10px;
}
.qpx-section__title {
  font-size: clamp(24px, 3vw, 36px); font-weight: 700;
  margin: 0 0 10px; color: var(--qpx-text-strong); letter-spacing: 0;
  line-height: 1.15;
}
.qpx-section__summary {
  font-size: clamp(15px, 1.3vw, 17px); color: var(--qpx-text-muted);
  margin: 0; max-width: 1160px;
}

.qpx-section__body {
  display: grid; gap: 20px;
  grid-template-columns: minmax(0, 1fr);
  max-width: 1160px;
  margin-bottom: 28px;
}
.qpx-section__body p { margin: 0; color: var(--qpx-text); }
.qpx-section__body p code {
  background: rgba(120, 150, 210, 0.1);
  padding: 2px 6px; border-radius: 3px;
  color: var(--qpx-link-hover);
}

.qpx-bullets {
  list-style: none; padding: 0; margin: 0 0 28px;
  display: grid; gap: 10px;
  max-width: 1160px;
}
.qpx-bullets li {
  position: relative; padding-left: 22px;
  color: var(--qpx-text); line-height: 1.55;
}
.qpx-bullets li::before {
  content: '▸'; position: absolute; left: 0; top: 0;
  color: var(--qpx-accent); font-weight: 700;
}
.qpx-bullets code {
  background: rgba(120, 150, 210, 0.1);
  padding: 2px 6px; border-radius: 3px;
  color: var(--qpx-link-hover);
}

.qpx-callout {
  padding: 16px 18px; border-left: 3px solid var(--qpx-accent);
  background: var(--qpx-surface-accent);
  border-radius: 0 4px 4px 0; max-width: 1160px;
  margin-bottom: 28px; color: var(--qpx-link-hover);
  font-size: 15px; line-height: 1.5;
}
.qpx-callout::before {
  content: 'i'; display: inline-block;
  width: 18px; height: 18px; line-height: 18px;
  text-align: center; background: var(--qpx-accent);
  color: #05070d; font-weight: 700; font-style: italic;
  font-family: Georgia, serif; border-radius: 50%;
  margin-right: 10px; vertical-align: text-bottom;
}

/* ─────────── IMAGE GRID ─────────── */

.qpx-images {
  display: grid; gap: 16px;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  margin-top: 8px;
}
.qpx-images--single {
  grid-template-columns: minmax(0, 1fr);
}
.qpx-shot {
  display: block; padding: 0; margin: 0;
  border: 1px solid var(--qpx-border-soft);
  border-radius: 6px; overflow: hidden;
  background: var(--qpx-surface-0);
  cursor: zoom-in; transition: all .2s ease;
  width: 100%; text-align: left;
}
.qpx-shot:hover {
  border-color: var(--qpx-border-accent);
  transform: translateY(-2px);
  box-shadow: 0 8px 24px var(--qpx-shadow);
}
.qpx-shot__frame {
  aspect-ratio: 16 / 10;
  background:
    repeating-linear-gradient(45deg,
      rgba(120, 150, 210, 0.05) 0 8px,
      rgba(120, 150, 210, 0.02) 8px 16px),
    var(--qpx-shot-bg);
  display: flex; align-items: center; justify-content: center;
  color: #4a5468;
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 12px; letter-spacing: 0.1em;
  overflow: hidden;
}
.qpx-shot__frame img {
  width: 100%; height: 100%; object-fit: contain;
}
.qpx-images--single .qpx-shot__frame {
  aspect-ratio: 16 / 9;
}
.qpx-shot__caption {
  padding: 10px 14px; font-size: 13px; color: var(--qpx-text-soft);
  border-top: 1px solid var(--qpx-border-soft);
  background: var(--qpx-surface-inset);
}

/* ─────────── DOCS ─────────── */

.qpx-docs {
  display: grid; gap: 16px;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  margin-top: 8px;
}
.qpx-doc-card {
  display: flex; flex-direction: column;
  padding: 20px; border: 1px solid var(--qpx-border-mid);
  border-radius: 6px;
  background: linear-gradient(180deg, var(--qpx-surface-0) 0%, rgba(255, 255, 255, 0) 100%);
  transition: all .2s ease;
  cursor: pointer;
}
.qpx-doc-card:hover {
  border-color: var(--qpx-border-accent);
  background: linear-gradient(180deg, rgba(110, 169, 255, 0.06) 0%, rgba(110, 169, 255, 0) 100%);
  transform: translateY(-2px);
}
.qpx-doc-card__title {
  font-size: 16px; font-weight: 700;
  margin: 0 0 6px; color: var(--qpx-text-strong);
}
.qpx-doc-card__desc {
  font-size: 14px; color: var(--qpx-text-soft);
  margin: 0 0 12px; flex: 1;
}
.qpx-doc-card__open {
  align-self: flex-start; font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 12px; font-weight: 700; color: var(--qpx-accent);
  letter-spacing: 0.1em; text-transform: uppercase;
}
.qpx-doc-card__open::after { content: ' →'; }

.qpx-release__actions {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
}
.qpx-release__button {
  min-height: 42px;
}
.qpx-release__quickstart-wrap {
  margin-top: 28px;
  display: grid;
  gap: 18px;
}
.qpx-release__quickstart-head {
  max-width: 1160px;
}
.qpx-release__quickstart-title {
  margin: 0 0 8px;
  font-size: 22px;
  line-height: 1.15;
  color: var(--qpx-text-strong);
}
.qpx-release__quickstart-summary {
  margin: 0;
  color: var(--qpx-text-soft);
}
.qpx-release__quickstart-grid {
  display: grid;
  gap: 14px;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
}
.qpx-release__quickstart-card {
  padding: 18px;
  border: 1px solid var(--qpx-border-soft);
  border-radius: 6px;
  background: linear-gradient(180deg, var(--qpx-surface-0) 0%, rgba(255, 255, 255, 0) 100%);
}
.qpx-release__quickstart-card-title {
  margin: 0 0 8px;
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 13px;
  font-weight: 700;
  letter-spacing: 0.04em;
  color: var(--qpx-text-strong);
}
.qpx-release__quickstart-card-text {
  margin: 0;
  color: var(--qpx-text);
  line-height: 1.6;
}
.qpx-release__quickstart-card-text code {
  background: rgba(120, 150, 210, 0.1);
  padding: 2px 6px;
  border-radius: 3px;
  color: var(--qpx-link-hover);
}

.qpx-beta__grid {
  display: grid;
  gap: 16px;
  grid-template-columns: minmax(0, 1.35fr) minmax(280px, 0.85fr);
  align-items: start;
}
.qpx-beta__card {
  padding: 22px;
  border: 1px solid var(--qpx-border-soft);
  border-radius: 8px;
  background: linear-gradient(180deg, var(--qpx-surface-1) 0%, rgba(255, 255, 255, 0) 100%);
  box-shadow: 0 10px 28px var(--qpx-shadow);
}
.qpx-beta__card-head {
  display: grid;
  gap: 6px;
  margin-bottom: 16px;
}
.qpx-beta__eyebrow {
  margin: 0;
  font-size: 11px;
  color: var(--qpx-accent);
  letter-spacing: 0.16em;
  text-transform: uppercase;
  font-family: 'JetBrains Mono', Consolas, monospace;
}
.qpx-beta__card-title {
  margin: 0;
  font-size: 22px;
  line-height: 1.15;
  color: var(--qpx-text-strong);
}
.qpx-beta__meta {
  margin: 0 0 18px;
  padding: 0;
  display: grid;
  gap: 10px;
}
.qpx-beta__meta-row {
  display: grid;
  grid-template-columns: minmax(96px, auto) 1fr;
  gap: 10px;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--qpx-border-soft);
}
.qpx-beta__meta-row:last-child {
  padding-bottom: 0;
  border-bottom: 0;
}
.qpx-beta__meta dt {
  margin: 0;
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--qpx-text-faint);
}
.qpx-beta__meta dd {
  margin: 0;
  color: var(--qpx-text-strong);
}
.qpx-beta__body {
  display: grid;
  gap: 10px;
}
.qpx-beta__body-title {
  margin: 0;
  font-size: 16px;
  color: var(--qpx-text-strong);
}
.qpx-beta__body p {
  margin: 0;
  color: var(--qpx-text);
}
.qpx-beta__actions {
  margin-top: 18px;
}
.qpx-beta__button {
  min-height: 42px;
}
.qpx-beta__notes {
  margin: 14px 0 0;
}

/* ─────────── FOOTER ─────────── */

.qpx-footer {
  padding: 32px 0; margin-top: 64px;
  border-top: 1px solid var(--qpx-border-soft);
  font-size: 13px; color: var(--qpx-text-faint);
}
.qpx-footer p { margin: 0 0 6px; }

/* ─────────── MODAL / LIGHTBOX ─────────── */

.qpx-modal {
  position: fixed; inset: 0; z-index: 100;
  display: none; align-items: center; justify-content: center;
  padding: 24px;
  background: var(--qpx-modal-backdrop);
  backdrop-filter: blur(4px);
  -webkit-backdrop-filter: blur(4px);
}
.qpx-modal.is-open { display: flex; }
body.qpx-modal-locked { overflow: hidden; }

.qpx-modal__inner {
  position: relative; max-width: min(1400px, 95vw); max-height: 92vh;
  display: flex; flex-direction: column; gap: 10px;
}
.qpx-modal__frame {
  border: 1px solid var(--qpx-border-strong);
  border-radius: 6px; overflow: hidden;
  background: var(--qpx-shot-bg); max-height: 80vh;
  display: flex; align-items: center; justify-content: center;
  position: relative;
}
.qpx-modal__img {
  max-width: 100%; max-height: 80vh;
  width: auto; height: auto;
  object-fit: contain;
}
.qpx-modal__nav {
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  width: 54px;
  height: 54px;
  border: 1px solid var(--qpx-border-strong);
  border-radius: 50%;
  background: var(--qpx-modal-utility-bg);
  color: var(--qpx-text-strong);
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-size: 36px;
  line-height: 1;
  z-index: 2;
  transition: all .15s ease;
}
.qpx-modal__nav:hover,
.qpx-modal__nav:focus-visible {
  border-color: var(--qpx-accent);
  background: rgba(110, 169, 255, 0.18);
}
.qpx-modal__nav[hidden] {
  display: none;
}
.qpx-modal__nav--prev {
  left: 18px;
}
.qpx-modal__nav--next {
  right: 18px;
}
.qpx-modal__placeholder {
  padding: 60px 40px; color: #4a5468;
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 13px; text-align: center;
}
.qpx-modal__caption {
  color: var(--qpx-text); font-size: 14px;
  text-align: center; min-height: 20px;
}
.qpx-modal__toolbar {
  position: absolute;
  top: -44px;
  right: 0;
  display: flex;
  align-items: center;
  gap: 8px;
}
.qpx-modal__close {
  position: static;
  width: 36px; height: 36px;
  border: 1px solid var(--qpx-border-strong);
  border-radius: 50%; background: var(--qpx-modal-utility-bg);
  color: var(--qpx-text-strong); font-size: 20px; line-height: 1;
  display: inline-flex; align-items: center; justify-content: center;
  transition: all .15s ease;
}
.qpx-modal__utility {
  width: 36px; height: 36px;
  border: 1px solid var(--qpx-border-strong);
  border-radius: 50%;
  background: var(--qpx-modal-utility-bg);
  color: var(--qpx-text-strong);
  font-size: 16px;
  line-height: 1;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  transition: all .15s ease;
}
.qpx-modal__utility:hover,
.qpx-modal__close:hover {
  border-color: var(--qpx-accent); background: rgba(110, 169, 255, 0.15);
}

.qpx-doc-modal__inner {
  width: min(1320px, 95vw);
  max-width: none;
  max-height: 92vh;
  gap: 0;
  background: var(--qpx-modal-bg);
  border: 1px solid var(--qpx-border-strong);
  border-radius: 10px;
  overflow: hidden;
}
.qpx-doc-modal__bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  padding: 16px 18px;
  border-bottom: 1px solid var(--qpx-border-mid);
  background: linear-gradient(180deg, rgba(110, 169, 255, 0.08), rgba(110, 169, 255, 0.025));
}
.qpx-doc-modal__meta {
  min-width: 0;
}
.qpx-doc-modal__eyebrow {
  margin: 0 0 4px;
  font-size: 11px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--qpx-accent);
  font-family: 'JetBrains Mono', Consolas, monospace;
}
.qpx-doc-modal__title {
  margin: 0;
  font-size: 18px;
  line-height: 1.2;
  color: var(--qpx-text-strong);
}
.qpx-doc-modal__actions {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-shrink: 0;
}
.qpx-doc-modal__download {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 38px;
  padding: 0 14px;
  border-radius: 6px;
  border: 1px solid var(--qpx-border-strong);
  background: var(--qpx-surface-0);
  color: var(--qpx-link-hover);
  font-size: 14px;
  font-weight: 600;
  transition: all .15s ease;
}
.qpx-doc-modal__download:hover,
.qpx-doc-modal__download:focus-visible {
  border-color: var(--qpx-accent);
  background: rgba(110, 169, 255, 0.12);
  color: var(--qpx-text-strong);
}
.qpx-doc-modal__close {
  position: static;
  top: auto;
  right: auto;
}
.qpx-doc-modal__frame {
  height: min(78vh, 980px);
  background: var(--qpx-shot-bg);
}
.qpx-doc-modal__iframe {
  width: 100%;
  height: 100%;
  display: block;
  border: 0;
  background: var(--qpx-shot-bg);
}

.qpx-note-modal__inner {
  width: min(620px, 94vw);
  background: var(--qpx-modal-bg);
  border: 1px solid var(--qpx-border-strong);
  border-radius: 12px;
  padding: 30px 24px 24px;
}
.qpx-note-modal__close {
  position: absolute;
  top: 14px;
  right: 14px;
}
.qpx-note-modal__body {
  display: grid;
  gap: 10px;
}
.qpx-note-modal__eyebrow {
  margin: 0;
  font-size: 11px;
  color: var(--qpx-accent);
  letter-spacing: 0.16em;
  text-transform: uppercase;
  font-family: 'JetBrains Mono', Consolas, monospace;
}
.qpx-note-modal__title {
  margin: 0;
  font-size: clamp(26px, 5vw, 34px);
  line-height: 1.1;
  color: var(--qpx-text-strong);
}
.qpx-note-modal__text {
  margin: 0;
  color: var(--qpx-text);
  line-height: 1.65;
}

/* ─────────── RESPONSIVE ─────────── */

@media (max-width: 820px) {
  .qpx-wrap { padding: 0 16px; }

  .qpx-nav-toggle { display: inline-flex; margin-left: auto; }
  .qpx-header__inner {
    gap: 0;
    padding-top: 10px;
    padding-bottom: 10px;
  }
  .qpx-header__top {
    min-height: 42px;
    flex-wrap: wrap;
    gap: 8px;
  }
  .qpx-nav {
    position: absolute; top: calc(100% - 1px); left: 0; right: 0;
    flex-direction: column; align-items: stretch;
    background: var(--qpx-header-bg);
    border-bottom: 1px solid var(--qpx-header-border);
    padding: 8px 16px 16px; gap: 2px;
    overflow-x: visible; max-height: 70vh; overflow-y: auto;
    display: none;
    z-index: 5;
  }
  .qpx-nav.is-open { display: flex; }
  .qpx-nav__item { padding: 10px 14px; font-size: 15px; }
  .qpx-nav__item--aux {
    display: block;
    border: 1px solid var(--qpx-border-mid);
    margin-top: 2px;
  }
  .qpx-nav__item--aux.qpx-nav__item--button {
    width: 100%;
    border: 1px solid var(--qpx-border-mid);
  }

  .qpx-brand__tag { display: none; }
  .qpx-brand { flex: 1 1 auto; }
  .qpx-header__right {
    flex: 1 1 100%;
    justify-content: flex-start;
    gap: 6px;
  }

  .qpx-docs-cta { display: none; }
  .qpx-social__label { display: none; }
  .qpx-social { padding: 0 10px; }
  .qpx-theme__label { display: none; }
  .qpx-theme { padding: 0 8px; }
  .qpx-theme__select { min-width: 72px; }

  .qpx-hero { padding: 48px 0 36px; }
  .qpx-hero__logo-wrap { margin: 0 0 6px; }
  .qpx-hero__logo { width: 62vw; max-width: 560px; }

  .qpx-section { padding: 48px 0; }
  .qpx-beta__grid { grid-template-columns: 1fr; }
}

@media (max-width: 1160px) {
  .qpx-brand__tag { display: none; }
}

@media (max-width: 480px) {
  .qpx-brand { font-size: 16px; letter-spacing: 0.04em; }
  .qpx-hero__logo {
    width: calc(100vw - 96px);
    max-width: 320px;
  }
  .qpx-hero__title {
    font-size: 30px;
    overflow-wrap: anywhere;
  }
  .qpx-hero__subtitle,
  .qpx-hero__release-note {
    overflow-wrap: anywhere;
  }
  .qpx-hero__meta-row {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
  }
  .qpx-hero__pillars { grid-template-columns: 1fr; }
  .qpx-hero__birth {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
  }
  .qpx-hero__birth-label { min-width: 0; }
  .qpx-hero__birth-value {
    flex: 1 1 100%;
    margin-left: 32px;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .qpx-images       { grid-template-columns: 1fr; }
  .qpx-docs         { grid-template-columns: 1fr; }

  .qpx-modal { padding: 12px; }
  .qpx-modal__toolbar { top: -36px; }
  .qpx-modal__close,
  .qpx-modal__utility { width: 32px; height: 32px; font-size: 18px; }
  .qpx-modal__nav {
    width: 44px;
    height: 44px;
    font-size: 30px;
  }
  .qpx-modal__nav--prev { left: 10px; }
  .qpx-modal__nav--next { right: 10px; }
}

@media (max-width: 820px) {
  .qpx-doc-modal__bar {
    flex-direction: column;
    align-items: stretch;
  }
  .qpx-doc-modal__actions {
    justify-content: space-between;
  }
  .qpx-doc-modal__download {
    flex: 1;
  }
  .qpx-doc-modal__frame {
    height: min(74vh, 760px);
  }
}

@media (prefers-reduced-motion: reduce) {
  html { scroll-behavior: auto; }
  *, *::before, *::after {
    transition-duration: 0.01ms !important;
    animation-duration: 0.01ms !important;
  }
}
`;

module.exports = { CSS };
