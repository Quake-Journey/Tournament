'use strict';

// Additional page layout only. Colours, fonts, controls and the lightbox
// come from the existing Q2PRO-X styles and client.
const CSS = `
.mg-header .qpx-header__right { flex-wrap:wrap; }
.mg-header .qpx-header__top { gap:20px; }
.mg-header .qpx-brand__tag { display:none; }
.mg-header .qpx-nav { display:flex; gap:6px; }
.mg-header .qpx-nav__item { font-size:13px; }
.mg-wordmark { width:246px; height:54px; }
.mg-wordmark--light { display:none; }
html[data-qpx-theme="light"] .mg-wordmark--dark { display:none; }
html[data-qpx-theme="light"] .mg-wordmark--light { display:block; }
.mg-hero { padding:64px 0 58px; }
.mg-hero-grid { display:grid; grid-template-columns:minmax(0,.94fr) minmax(0,1.06fr); align-items:center; gap:52px; }
.mg-kicker { color:var(--qpx-accent); font:12px/1.7 Consolas,monospace; letter-spacing:.06em; margin:0 0 20px; }
.mg-hero h1 { color:var(--qpx-text-strong); font-size:clamp(34px,3.7vw,52px); line-height:1.1; letter-spacing:-.035em; text-wrap:balance; margin:0 0 24px; }
.mg-intro { font-size:17px; line-height:1.75; margin:0 0 18px; }
.mg-base { font-size:14px; color:var(--qpx-text-muted); margin:0 0 28px; }
.mg-actions { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }
.mg-button { display:inline-flex; justify-content:center; align-items:center; gap:10px; padding:12px 19px; border-radius:7px; border:1px solid var(--qpx-border-accent); color:var(--qpx-text-strong); background:var(--qpx-surface-accent); font-size:14px; font-weight:600; }
.mg-button--primary { color:#fff; background:#295bdc; border-color:#295bdc; }
.mg-button--primary:hover,.mg-button--primary:focus { color:#fff; background:#204cbb; }
.mg-download-note { margin:12px 0 0; font-size:12px; color:var(--qpx-text-muted); }
.mg-screen { margin:0; min-width:0; }
.mg-screen .qpx-shot { width:100%; text-align:left; background:var(--qpx-surface-1); border:1px solid var(--qpx-border-mid); border-radius:12px; padding:0; overflow:hidden; }
.mg-screen .qpx-shot__frame { aspect-ratio:16/9; background:var(--qpx-shot-bg); }
.mg-screen .qpx-shot__frame img { width:100%; height:100%; object-fit:contain; }
.mg-screen .qpx-shot__caption { padding:14px 18px; line-height:1.6; font-size:13px; color:var(--qpx-text-muted); }
.mg-hero .mg-screen { box-shadow:0 18px 70px var(--qpx-shadow); border-radius:12px; }
.mg-hint { font-size:11px; color:var(--qpx-text-soft); margin:10px 0 0; text-align:right; }
.mg-section { padding:52px 0; border-top:1px solid var(--qpx-border-soft); scroll-margin-top:130px; }
.mg-section h2 { font-size:clamp(27px,3vw,36px); line-height:1.2; color:var(--qpx-text-strong); letter-spacing:-.02em; text-wrap:balance; margin:0 0 18px; }
.mg-section-intro { font-size:16px; line-height:1.75; color:var(--qpx-text-muted); max-width:730px; margin:0 0 30px; }
.mg-feature-grid { display:grid; grid-template-columns:minmax(0,.85fr) minmax(0,1.15fr); gap:48px; align-items:center; }
.mg-feature-list { display:grid; gap:23px; margin:0; }
.mg-feature-list dt { font-size:17px; font-weight:650; color:var(--qpx-accent); margin-bottom:6px; }
.mg-feature-list dd { margin:0; line-height:1.75; font-size:15px; }
.mg-gallery { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:24px; }
.mg-steps { padding:0; margin:0; list-style:none; counter-reset:steps; display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:28px; }
.mg-steps li { counter-increment:steps; padding:20px 0 0; border-top:1px solid var(--qpx-border-accent); }
.mg-steps li::before { content:counter(steps,decimal-leading-zero); display:block; font:14px/1 Consolas,monospace; color:var(--qpx-accent); margin-bottom:18px; }
.mg-steps h3 { margin:0 0 10px; font-size:17px; color:var(--qpx-text-strong); }
.mg-steps p { font-size:14px; line-height:1.8; margin:0; color:var(--qpx-text-muted); overflow-wrap:anywhere; }
.mg-requirements { display:grid; grid-template-columns:minmax(0,1fr) minmax(0,1fr); gap:48px; }
.mg-requirements ul { padding:0; margin:0; list-style:none; display:grid; gap:20px; }
.mg-requirements li { border-left:2px solid var(--qpx-border-accent); padding-left:18px; }
.mg-requirements strong { display:block; color:var(--qpx-text-strong); font-size:16px; margin-bottom:7px; }
.mg-requirements li p { font-size:14px; line-height:1.8; margin:0; color:var(--qpx-text-muted); }
.mg-note { padding:24px; border:1px solid var(--qpx-border-mid); border-radius:10px; background:var(--qpx-surface-accent); align-self:start; }
.mg-note p { margin:0 0 16px; font-size:15px; line-height:1.8; }
.mg-note p:last-child { margin:0; }
.mg-note small { font-size:13px; color:var(--qpx-text-muted); }
.mg-resource-links { display:flex; gap:18px; flex-wrap:wrap; margin:22px 0; font-size:14px; }
.mg-community { padding-top:22px; margin-top:30px; border-top:1px solid var(--qpx-border-mid); }
.mg-footer { border-top:1px solid var(--qpx-border-soft); padding:24px 0; color:var(--qpx-text-soft); font-size:12px; }
.mg-footer .qpx-wrap { display:flex; justify-content:space-between; gap:20px; flex-wrap:wrap; }
@media(max-width:1000px) {
 .mg-hero-grid { grid-template-columns:1fr; gap:32px; }
 .mg-hero { padding:38px 0; }
 .mg-hero h1 { max-width:780px; }
 .mg-intro { max-width:820px; }
 .mg-steps { grid-template-columns:repeat(2,minmax(0,1fr)); }
 .mg-header .qpx-header__top { flex-wrap:wrap; }
}
@media(max-width:820px) {
 .mg-header .qpx-docs-cta { display:inline-flex; }
 .mg-header .qpx-nav { display:none; }
 .mg-header .qpx-nav.is-open { display:flex; flex-wrap:wrap; }
 .mg-header .qpx-nav-toggle { display:block; }
 .mg-header .qpx-header__right { margin-left:0; }
 .mg-feature-grid,.mg-requirements { grid-template-columns:1fr; gap:30px; }
 .mg-gallery { grid-template-columns:1fr; }
 .mg-section { padding:36px 0; }
}
@media(max-width:480px) {
 .mg-header .qpx-header__right { width:100%; justify-content:space-between; gap:8px; }
 .mg-header .qpx-docs-cta { padding:8px 10px; font-size:12px; }
 .mg-wordmark { width:214px; height:47px; }
 .mg-hero h1 { font-size:34px; }
 .mg-steps { grid-template-columns:1fr; gap:20px; }
 .mg-hero .mg-actions .mg-button { width:100%; }
}
@media(prefers-reduced-motion:reduce) { html { scroll-behavior:auto; } }
`;
module.exports = { CSS };
