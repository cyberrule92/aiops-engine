#!/usr/bin/env python3
"""Convert body.md -> standalone branded HTML, embedding rendered diagram
PNGs (base64 inline) in place of @@DIAGRAM_NN@@ tokens. Colour system,
cover page, and running header/footer."""
import base64, pathlib, re
import markdown

# ---- Document palette ---------------------------------------------------
BRAND_PRIMARY    = "#0E7490"   # primary
BRAND_PRIMARY_DK = "#155E75"   # darker shade for text/rules
BRAND_SLATE      = "#334155"   # dark gray
BRAND_INK        = "#1F2937"   # body text
BRAND_TINT       = "#ECFEFF"   # light fill
BRAND_TINT2      = "#CFFAFE"
BRAND_ORANGE     = "#EA580C"   # accent
BRAND_PURPLE     = "#7C3AED"   # accent

# Document identity shown on the cover and running header.
DOC_TITLE_MARK   = "AIOps Intelligence Engine"

body = pathlib.Path("body.md").read_text()

md = markdown.Markdown(extensions=[
    "extra", "tables", "fenced_code", "codehilite", "toc", "sane_lists", "admonition"
], extension_configs={"codehilite": {"guess_lang": False, "noclasses": False}})
html_body = md.convert(body)

def embed(m):
    n = m.group(1)
    p = pathlib.Path(f"diagrams/diagram_{n}.png")
    if not p.exists():
        return f'<div class="diagram missing">[diagram {n} missing]</div>'
    b64 = base64.b64encode(p.read_bytes()).decode()
    return (f'<figure class="diagram">'
            f'<img src="data:image/png;base64,{b64}" alt="diagram {n}"/>'
            f'<figcaption>Figure {int(n)}</figcaption></figure>')

html_body = re.sub(r'(?:<p>)?@@DIAGRAM_(\d+)@@(?:</p>)?', embed, html_body)

# ---- Document wordmark, as inline SVG -----------------------------------
def doc_wordmark(width_px, stacked=True):
    """Accent rule + the document's own title, used on the cover and header."""
    if stacked:
        return f'''
<svg width="{width_px}" viewBox="0 0 320 86" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{DOC_TITLE_MARK}">
  <rect x="0" y="0" width="150" height="34" fill="{BRAND_PRIMARY}"/>
  <text x="0" y="66" font-family="Arial,Helvetica,sans-serif" font-weight="300"
        font-size="20" letter-spacing="0.3" fill="{BRAND_INK}">AIOps Intelligence</text>
  <text x="0" y="86" font-family="Arial,Helvetica,sans-serif" font-weight="300"
        font-size="20" letter-spacing="0.3" fill="{BRAND_INK}">Engine</text>
</svg>'''
    # inline compact (header)
    return f'''
<svg width="{width_px}" viewBox="0 0 360 30" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{DOC_TITLE_MARK}">
  <rect x="0" y="2" width="74" height="17" fill="{BRAND_PRIMARY}"/>
  <text x="84" y="16" font-family="Arial,Helvetica,sans-serif" font-weight="300"
        font-size="14" letter-spacing="0.3" fill="{BRAND_INK}">{DOC_TITLE_MARK}</text>
</svg>'''

CSS = f"""
@page {{ size: A4; margin: 24mm 16mm 18mm 16mm; }}
* {{ box-sizing: border-box; }}
body {{ font-family: "Arial","Helvetica Neue",Helvetica,sans-serif;
       font-size: 10.5pt; line-height: 1.5; color: {BRAND_INK}; margin: 0; }}

/* Cover */
.cover {{ page-break-after: always; padding: 0; height: 232mm; position: relative; }}
.cover .logo {{ margin: 8mm 0 0 2mm; }}
.cover .ttl-wrap {{ margin: 50mm 2mm 0; }}
.cover .cover-accent {{ position: absolute; bottom: 0; left: 0; width: 70mm; height: 8mm;
       background: {BRAND_PRIMARY}; }}
.cover .kicker {{ color: {BRAND_PRIMARY_DK}; font-weight: 700; letter-spacing: 3px;
       font-size: 11pt; text-transform: uppercase; }}
.cover h1 {{ font-size: 34pt; color: {BRAND_SLATE}; font-weight: 700; line-height: 1.1;
       margin: 4mm 0 0; border: 0; padding: 0; }}
.cover .sub {{ font-size: 13pt; color: {BRAND_SLATE}; max-width: 150mm; margin: 8mm 0 0;
       font-weight: 300; }}
.cover .rule {{ width: 60mm; height: 4px; background: {BRAND_PRIMARY}; margin: 10mm 0; }}
.cover .meta {{ font-size: 10pt; color: {BRAND_SLATE}; }}
.cover .meta b {{ color: {BRAND_INK}; }}
.cover .footer {{ position: absolute; bottom: 0; left: 0; right: 0; height: 8mm;
       background: {BRAND_SLATE}; color: #fff; font-size: 8pt; display: flex;
       align-items: center; padding: 0 4mm; justify-content: space-between; }}

/* Headings */
h1 {{ font-size: 18pt; color: {BRAND_SLATE}; font-weight: 700;
     border-bottom: 3px solid {BRAND_PRIMARY}; padding-bottom: 3px;
     margin-top: 11mm; page-break-after: avoid; }}
h2 {{ font-size: 14pt; color: {BRAND_PRIMARY_DK}; font-weight: 700; margin-top: 8mm;
     page-break-after: avoid; border-bottom: 1px solid #d7e3e0; padding-bottom: 2px; }}
h3 {{ font-size: 12pt; color: {BRAND_SLATE}; font-weight: 700; margin-top: 6mm; page-break-after: avoid; }}
h4 {{ font-size: 11pt; color: {BRAND_SLATE}; page-break-after: avoid; }}
p, li {{ orphans: 2; widows: 2; }}
a {{ color: {BRAND_PRIMARY_DK}; text-decoration: none; }}

/* Code */
code {{ font-family: "SFMono-Regular",Consolas,"Liberation Mono",monospace;
       font-size: 9pt; background: {BRAND_TINT}; padding: 1px 4px; border-radius: 3px;
       color: {BRAND_PRIMARY_DK}; }}
pre {{ background: {BRAND_SLATE}; color: #eaf1f0; padding: 10px 12px; border-radius: 6px;
      font-size: 8.6pt; line-height: 1.4; overflow: auto; page-break-inside: avoid;
      border-left: 4px solid {BRAND_PRIMARY}; }}
pre code {{ background: none; color: inherit; padding: 0; }}
.codehilite {{ background: {BRAND_SLATE}; border-radius: 6px; page-break-inside: avoid;
      border-left: 4px solid {BRAND_PRIMARY}; }}
.codehilite pre {{ margin: 0; border-left: 0; }}

/* Tables */
table {{ border-collapse: collapse; width: 100%; margin: 4mm 0; font-size: 9pt;
        page-break-inside: avoid; }}
th {{ background: {BRAND_PRIMARY}; color: #fff; text-align: left; padding: 6px 9px; font-weight: 700; }}
td {{ border: 1px solid #d7e3e0; padding: 5px 9px; vertical-align: top; }}
tr:nth-child(even) td {{ background: {BRAND_TINT}; }}

blockquote {{ border-left: 4px solid {BRAND_PRIMARY}; background: {BRAND_TINT};
             margin: 4mm 0; padding: 5px 14px; color: {BRAND_SLATE}; border-radius: 0 4px 4px 0; }}

figure.diagram {{ text-align: center; margin: 6mm 0; page-break-inside: avoid; }}
figure.diagram img {{ max-width: 100%; height: auto; border: 1px solid {BRAND_TINT};
             border-radius: 6px; padding: 8px; background: #fff; }}
figure.diagram figcaption {{ font-size: 8.5pt; color: {BRAND_SLATE}; margin-top: 2mm;
             font-style: italic; }}
hr {{ border: 0; border-top: 1px solid #d7e3e0; margin: 8mm 0; }}
.toc {{ background: {BRAND_TINT}; border: 1px solid #a5f3fc; border-left: 4px solid {BRAND_PRIMARY};
       border-radius: 0 6px 6px 0; padding: 6px 18px; }}
strong {{ color: {BRAND_INK}; }}
"""

try:
    from pygments.formatters import HtmlFormatter
    pyg = HtmlFormatter(style="monokai").get_style_defs(".codehilite")
except Exception:
    pyg = ""

cover = f"""
<div class="cover">
  <div class="logo">{doc_wordmark(230, stacked=True)}</div>
  <div class="ttl-wrap">
    <div class="kicker">Solution Design &middot; HLD + LLD</div>
    <h1>AIOps Intelligence Engine</h1>
    <div class="sub">AI-powered anomaly detection, alert correlation, causal topology,
    predictive forecasting &amp; LLM root-cause analysis for Kubernetes — air-gapped.</div>
    <div class="rule"></div>
    <div class="meta">
      <b>Version</b> 1.0 &nbsp;&middot;&nbsp; <b>Status</b> Production-Readiness Baseline<br/>
      <b>Date</b> 2026-06-02
    </div>
  </div>
  <div class="cover-accent"></div>
</div>
"""

html = f"""<!doctype html><html><head><meta charset="utf-8">
<style>{CSS}\n{pyg}</style></head>
<body>{cover}{html_body}</body></html>"""

pathlib.Path("document.html").write_text(html)
# header/footer fragments for puppeteer
pathlib.Path("header.html").write_text(doc_wordmark(150, stacked=False))
print(f"wrote document.html ({len(html)//1024} KB) + header.html")
