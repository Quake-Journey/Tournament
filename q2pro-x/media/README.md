# Q2PRO-X website media

One folder per feature section, served from `/q2pro-x/media/<folder>/<file>`.

## Folder → section

| Folder             | Section on `/q2pro-x`   | Anchor              |
|--------------------|-------------------------|---------------------|
| `home/`            | Overview (hero)         | `#overview`         |
| `laghax/`          | LAGHAX                  | `#laghax`           |
| `weapon-predict/`  | Weapon Predict          | `#weapon-predict`   |
| `movement-physics/`| Movement & Physics      | `#movement-physics` |
| `sound/`           | Sound & Acoustics       | `#sound`            |
| `visuals/`         | Visuals & Highlights    | `#visuals`          |
| `overlays/`        | Overlays & Tools        | `#overlays`         |

## File naming

Use a 2-digit prefix so listings stay ordered:

```
01-overview.png
02-overlay.png
03-debug.png
```

## How new screenshots land on the page

1. Drop the file into the matching `media/<folder>/` directory using the
   filename referenced in `../content.js` (the `images[].file` field).
2. Reload `/q2pro-x` — the image tile picks up the new file automatically.
   No render / route changes required.

If the filename you need doesn't exist in `content.js` yet, add a new
entry to the matching section's `images: [ ... ]` array:

```js
{
  file: '04-new-shot.png',
  alt:     { ru: 'Описание alt', en: 'Alt description' },
  caption: { ru: 'Подпись',      en: 'Caption' },
},
```

That's the only file change — the render loop iterates `images` so the
tile and the lightbox entry show up automatically on the next refresh.

## Image format

- **PNG** or **JPG**; PNG preferred for UI screenshots with sharp text.
- Full in-game resolution is fine — the tile uses CSS `object-fit: cover`
  with `aspect-ratio: 16/10`, and the lightbox scales to fit the viewport.
- Keep file sizes sensible (ideally < 400 KB each); nothing on the page
  preloads them, but a long section does lazy-load a grid of several.

## Placeholder behaviour

If a referenced file is missing, the page still renders. The tile shows
the filename in a subtle pattern, and the lightbox displays a textual
"image not added yet: <filename>" placeholder. This means the site is
always publishable — never broken by a missing screenshot.
