from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import tempfile
import uuid
from html import escape
from pathlib import Path

from docx import Document
from docx.oxml.ns import qn
from docx.table import Table
from docx.text.paragraph import Paragraph


PRODUCT = "Q2PRO-X 1.5"
PRODUCT_COMMIT = "28b0f5338d138dbf3cb70c789fad85787a9af63a"
RELEASE_DATE_ISO = "2026-08-25"
DEFAULT_SITE_ROOT = Path(r"O:\Claude\public\q2pro-x")
DEFAULT_STAGE_PARENT = Path(
    r"O:\Claude2\_agent_temp\codex\q2prox-site-docs-1.5-stage-20260825"
)

SUBJECTS = [
    "Documentation_Index",
    "Release_Notes",
    "Interface_Menu_Console_Guide",
    "Cvar_Command_Reference",
    "Cvars_Cvar_Browser_Guide",
    "Video_Visuals_Guide",
    "HDR_Guide",
    "Sound_Guide",
    "Voice_Chat_Guide",
    "Controls_Mouse_Zoom_Guide",
    "Network_Play_Guide",
    "Network_God",
    "Demo_Browser_Player_Guide",
    "Game_Statistics_Demo_Auto_Statistics_Guide",
    "Live_Item_Timers_Guide",
    "Startup_Intro_Settings_Guide",
    "AVFX_Settings_Guide",
    "AVFX_Presets_Guide",
    "AVFX_Advanced_Features_Guide",
    "AVFX_Preset_Generator_Guide",
    "Visual_Highlights_Guide",
    "Single_Player_Mods_Guide",
]


def filename(subject: str, lang: str) -> str:
    if subject == "Network_God" and lang == "ru":
        return "Q2PRO-X_1.5_Network_God_Руководство_RU.docx"
    suffix = "RU" if lang == "ru" else "EN"
    if subject == "Network_God":
        subject = "Network_God_Guide"
    return f"Q2PRO-X_1.5_{subject}_{suffix}.docx"


EXPECTED = {
    lang: {filename(subject, lang) for subject in SUBJECTS}
    for lang in ("ru", "en")
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def image_data_uri(path: Path) -> str:
    if not path.is_file():
        raise RuntimeError(f"Missing preserved adaptive wordmark: {path}")
    mime = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"


def read_manifest(source: Path) -> dict:
    manifest_path = source.parent / "final_manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(f"Missing authoritative manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("product") != PRODUCT:
        raise RuntimeError(f"Wrong manifest product: {manifest.get('product')!r}")
    if manifest.get("product_commit") != PRODUCT_COMMIT:
        raise RuntimeError(
            f"Wrong product commit: {manifest.get('product_commit')!r}"
        )
    if manifest.get("documents") != 44 or manifest.get("validator_pass") != 44:
        raise RuntimeError("Manifest is not a complete 44/44 validated set")
    if manifest.get("failures"):
        raise RuntimeError(f"Manifest carries failures: {manifest['failures']!r}")
    return manifest


def validate_source(source: Path, manifest: dict) -> list[Path]:
    rows = {row["file"]: row for row in manifest.get("rows", [])}
    if set(rows) != EXPECTED["ru"] | EXPECTED["en"]:
        raise RuntimeError("Manifest filenames do not equal the fixed 22 RU + 22 EN set")

    result: list[Path] = []
    total_bytes = 0
    forbidden = re.compile(
        r"Q2PRO-X(?:\s+|_)1\.[0-4](?!\d)|KEEPS\s+1\.4|"
        r"2026-08-(?:23|24)|August (?:23|24), 2026|(?:23|24) августа 2026|"
        r'TargetMode="External"|INCLUDEPICTURE|INCLUDETEXT|DDEAUTO|<o:OLEObject',
        re.IGNORECASE,
    )

    import zipfile

    for lang in ("ru", "en"):
        lang_dir = source / lang
        actual = {path.name for path in lang_dir.glob("*.docx") if path.is_file()}
        if actual != EXPECTED[lang]:
            missing = sorted(EXPECTED[lang] - actual)
            extra = sorted(actual - EXPECTED[lang])
            raise RuntimeError(f"{lang}: missing={missing!r}, extra={extra!r}")

        for name in sorted(EXPECTED[lang]):
            path = lang_dir / name
            row = rows[name]
            actual_hash = sha256(path)
            if actual_hash != row["sha256"].upper():
                raise RuntimeError(f"Manifest SHA mismatch: {path}")
            if path.stat().st_size != row["bytes"]:
                raise RuntimeError(f"Manifest size mismatch: {path}")
            total_bytes += path.stat().st_size

            with zipfile.ZipFile(path) as package:
                package_text = "".join(
                    package.read(part).decode("utf-8", errors="replace")
                    for part in package.namelist()
                    if part.endswith(".xml") or part.endswith(".rels")
                )
            match = forbidden.search(package_text)
            if match:
                raise RuntimeError(f"Forbidden package token {match.group(0)!r}: {path}")
            if "<w:updateFields w:val=\"false\"" not in package_text:
                raise RuntimeError(f"Explicit updateFields=false is absent: {path}")
            if "<LinksUpToDate>true</LinksUpToDate>" not in package_text:
                raise RuntimeError(f"LinksUpToDate is not true: {path}")
            if "<HyperlinksChanged>false</HyperlinksChanged>" not in package_text:
                raise RuntimeError(f"HyperlinksChanged is not false: {path}")
            if f"{RELEASE_DATE_ISO}T12:00:00Z" not in package_text:
                raise RuntimeError(f"Release date properties are absent: {path}")
            result.append(path)

    if total_bytes != manifest.get("total_bytes"):
        raise RuntimeError(
            f"Total-byte mismatch: files={total_bytes}, manifest={manifest.get('total_bytes')}"
        )
    return result


def block_items(document: Document):
    for child in document.element.body.iterchildren():
        if child.tag == qn("w:p"):
            yield Paragraph(child, document)
        elif child.tag == qn("w:tbl"):
            yield Table(child, document)


def run_html(run_element, paragraph: Paragraph) -> str:
    chunks: list[str] = []
    for node in run_element.iter():
        if node.tag == qn("w:t"):
            chunks.append(escape(node.text or ""))
        elif node.tag == qn("w:tab"):
            chunks.append("&emsp;")
        elif node.tag == qn("w:br"):
            chunks.append("<br>")
        elif node.tag == qn("a:blip"):
            rel_id = node.get(qn("r:embed"))
            if rel_id and rel_id in paragraph.part.related_parts:
                part = paragraph.part.related_parts[rel_id]
                mime = getattr(part, "content_type", "image/png")
                data = base64.b64encode(part.blob).decode("ascii")
                chunks.append(
                    f'<img class="doc-image" src="data:{escape(mime)};base64,{data}" '
                    'alt="Q2PRO-X document image">'
                )
    value = "".join(chunks)
    properties = run_element.find(qn("w:rPr"))
    if properties is not None:
        if properties.find(qn("w:b")) is not None:
            value = f"<strong>{value}</strong>"
        if properties.find(qn("w:i")) is not None:
            value = f"<em>{value}</em>"
    return value


def paragraph_inner_html(paragraph: Paragraph) -> str:
    chunks: list[str] = []
    for child in paragraph._p:
        if child.tag == qn("w:r"):
            chunks.append(run_html(child, paragraph))
        elif child.tag == qn("w:hyperlink"):
            inner = "".join(run_html(run, paragraph) for run in child if run.tag == qn("w:r"))
            anchor = child.get(qn("w:anchor"))
            if anchor:
                chunks.append(f'<a href="#{escape(anchor)}">{inner}</a>')
            else:
                chunks.append(inner)
    return "".join(chunks).strip()


def paragraph_bookmark(paragraph: Paragraph) -> str:
    for node in paragraph._p.iter(qn("w:bookmarkStart")):
        name = node.get(qn("w:name"), "")
        if name and not name.startswith("_"):
            return name
    return ""


def render_paragraph(paragraph: Paragraph) -> str:
    body = paragraph_inner_html(paragraph)
    if not body:
        return ""
    style = (getattr(getattr(paragraph, "style", None), "name", "") or "").lower()
    anchor = paragraph_bookmark(paragraph)
    ident = f' id="{escape(anchor)}"' if anchor else ""
    if "heading 1" in style or "заголовок 1" in style:
        return f"<h1{ident}>{body}</h1>"
    if "heading 2" in style or "заголовок 2" in style:
        return f"<h2{ident}>{body}</h2>"
    if "heading 3" in style or "заголовок 3" in style:
        return f"<h3{ident}>{body}</h3>"
    if "title" in style or "название" in style:
        return f"<h1{ident}>{body}</h1>"
    if "list bullet" in style or "маркирован" in style:
        return f'<div class="doc-list-item doc-list-item--bullet"{ident}>{body}</div>'
    if "list number" in style or "нумер" in style:
        return f'<div class="doc-list-item doc-list-item--number"{ident}>{body}</div>'
    return f"<p{ident}>{body}</p>"


def render_table(table: Table) -> str:
    rows: list[str] = []
    for row_index, row in enumerate(table.rows):
        cells: list[str] = []
        tag = "th" if row_index == 0 else "td"
        for cell in row.cells:
            paragraphs = [paragraph_inner_html(paragraph) for paragraph in cell.paragraphs]
            value = "<br>".join(value for value in paragraphs if value)
            cells.append(f"<{tag}>{value}</{tag}>")
        rows.append(f"<tr>{''.join(cells)}</tr>")
    return f'<div class="doc-table-wrap"><table>{"".join(rows)}</table></div>'


def preview_html(source: Path, lang: str, wordmark_uri: str) -> str:
    document = Document(source)
    title = document.core_properties.title or source.stem.replace("_", " ")
    chunks: list[str] = []
    for block in block_items(document):
        if isinstance(block, Paragraph):
            rendered = render_paragraph(block)
        else:
            rendered = render_table(block)
        if rendered:
            chunks.append(rendered)

    lead = (
        "Полная локальная HTML-версия принятого документа. Оригинал DOCX доступен в карточке документации."
        if lang == "ru"
        else "Complete local HTML rendering of the accepted document. The original DOCX is available from its documentation card."
    )
    return f"""<!doctype html>
<html lang="{lang}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{escape(title)}</title>
  <meta name="description" content="{escape(lead)}">
  <style>
    :root{{--bg:#080b12;--panel:#111725;--text:#dce5f3;--muted:#9aa9bf;--line:#2b405d;--accent:#72e2d2}}
    *{{box-sizing:border-box}} html{{scroll-behavior:smooth}} body{{margin:0;background:radial-gradient(circle at 15% 0,#17243a 0,transparent 38%),var(--bg);color:var(--text);font:16px/1.62 Inter,"Segoe UI",Arial,sans-serif}}
    .wrap{{width:min(1120px,calc(100vw - 28px));margin:0 auto;padding:22px 0 56px}} .card{{overflow:hidden;border:1px solid var(--line);border-radius:16px;background:rgba(17,23,37,.97);box-shadow:0 24px 70px #0008}}
    header{{padding:24px;border-bottom:1px solid var(--line);text-align:center}} header img{{display:block;width:min(520px,100%);margin:0 auto 16px}} header h1{{margin:0;font-size:clamp(25px,4vw,40px)}} header p{{max-width:800px;margin:10px auto 0;color:var(--muted)}} main{{padding:28px}}
    h1,h2,h3{{scroll-margin-top:20px;line-height:1.2;color:#f3f7ff}} h1{{margin:1.5em 0 .55em;font-size:2rem}} h2{{margin:1.35em 0 .5em;font-size:1.45rem}} h3{{margin:1.2em 0 .45em;font-size:1.15rem}} p{{margin:0 0 .9em}} a{{color:var(--accent);text-decoration-thickness:1px;text-underline-offset:3px}}
    .doc-list-item{{position:relative;margin:.28em 0 .45em;padding-left:1.45em}} .doc-list-item--bullet:before{{content:"•";position:absolute;left:.28em;color:var(--accent)}} .doc-list-item--number{{counter-increment:list-item}} .doc-list-item--number:before{{content:counter(list-item) ".";position:absolute;left:0;color:var(--accent)}}
    .doc-table-wrap{{overflow:auto;margin:1em 0 1.35em;border:1px solid var(--line);border-radius:12px}} table{{width:100%;min-width:640px;border-collapse:collapse}} th,td{{padding:10px 12px;border-right:1px solid var(--line);border-bottom:1px solid var(--line);vertical-align:top;text-align:left}} th{{background:#18243a;color:#f3f7ff}} tr:last-child td{{border-bottom:0}} th:last-child,td:last-child{{border-right:0}}
    .doc-image{{display:block;max-width:100%;height:auto;margin:18px auto;border-radius:10px}} code{{padding:.08em .3em;border-radius:5px;background:#1a2941;color:#edf7ff}} @media(max-width:700px){{.wrap{{width:min(100%,calc(100vw - 16px));padding-top:8px}}header,main{{padding:17px}}h1{{font-size:1.55rem}}}}
  </style>
</head>
<body><div class="wrap"><article class="card"><header>
  <img src="{wordmark_uri}" alt="Q2PRO-X">
  <h1>{escape(title)}</h1><p>{escape(lead)}</p>
</header><main>{''.join(chunks)}</main></article></div></body></html>"""


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    shutil.copy2(source, temporary)
    os.replace(temporary, destination)


def build_stage(source: Path, files: list[Path], stage_parent: Path, site_root: Path) -> Path:
    stage_parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix="candidate-", dir=stage_parent))
    wordmark_uri = image_data_uri(
        site_root / "branding" / "Q2PRO-X_1_0_wordmark_dark.png"
    )
    for lang in ("ru", "en"):
        (stage / "docs" / lang).mkdir(parents=True)
        (stage / "docs-preview" / lang).mkdir(parents=True)
    for path in files:
        lang = path.parent.name
        shutil.copy2(path, stage / "docs" / lang / path.name)
        preview = stage / "docs-preview" / lang / f"{path.stem}.html"
        preview.write_text(preview_html(path, lang, wordmark_uri), encoding="utf-8")
    return stage


def install(stage: Path, site_root: Path, manifest: dict) -> None:
    # Previews land first. The website exposes no documentation cards until all
    # 22 DOCX files for a language exist, so an interrupted copy stays hidden.
    for lang in ("ru", "en"):
        previews = sorted((stage / "docs-preview" / lang).glob("*.html"))
        docs = sorted((stage / "docs" / lang).glob("*.docx"))
        if len(previews) != 22 or len(docs) != 22:
            raise RuntimeError(f"Incomplete staged language {lang}")
        for path in previews:
            atomic_copy(path, site_root / "docs-preview" / lang / path.name)
        for path in docs:
            atomic_copy(path, site_root / "docs" / lang / path.name)

    rows = {row["file"]: row for row in manifest["rows"]}
    for lang in ("ru", "en"):
        for name in EXPECTED[lang]:
            installed = site_root / "docs" / lang / name
            preview = site_root / "docs-preview" / lang / f"{Path(name).stem}.html"
            if sha256(installed) != rows[name]["sha256"].upper():
                raise RuntimeError(f"Installed DOCX hash mismatch: {installed}")
            if not preview.is_file() or preview.stat().st_size == 0:
                raise RuntimeError(f"Installed preview is missing: {preview}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate and install the complete accepted Q2PRO-X 1.5 website documentation set."
    )
    parser.add_argument("--source", required=True, type=Path, help="Candidate _stage/docs directory")
    parser.add_argument("--site-root", type=Path, default=DEFAULT_SITE_ROOT)
    parser.add_argument("--stage-parent", type=Path, default=DEFAULT_STAGE_PARENT)
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--stage-only", action="store_true")
    args = parser.parse_args()

    source = args.source.resolve()
    manifest = read_manifest(source)
    files = validate_source(source, manifest)
    print(
        f"VALID source={source} documents={len(files)} bytes={manifest['total_bytes']} "
        f"commit={manifest['product_commit']}"
    )
    if args.check_only:
        return

    stage = build_stage(
        source, files, args.stage_parent.resolve(), args.site_root.resolve()
    )
    if args.stage_only:
        print(f"STAGED stage_kept={stage}")
        return
    install(stage, args.site_root.resolve(), manifest)
    print(f"INSTALLED site={args.site_root.resolve()} stage_kept={stage}")


if __name__ == "__main__":
    main()
