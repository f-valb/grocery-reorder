"""Make the real-format source documents: a PDF contract, a Word visit note and a photographed delivery note.

    ~/.local/venvs/pyspark/bin/python scripts/make_source_files.py

Writes to docs/source_files/ (committed, so the demo files are reviewable). Uses only matplotlib (PDF with real,
selectable text), the standard library (a .docx is zipped XML) and PIL (an image: the text is pixels and needs OCR).
Numbers match the generated data: ST-07 ordered 264 units of Coffee for the week of 2026-10-26 and received 96.
"""

import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.backends.backend_pdf import PdfPages  # noqa: E402
from PIL import Image, ImageDraw, ImageFilter, ImageFont  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "docs" / "source_files"
OUT.mkdir(parents=True, exist_ok=True)
plt.rcParams["pdf.fonttype"] = 42  # embed TrueType so the PDF text stays selectable

# --- PDF: Coffee supply agreement, 2 pages, the allocation clauses on page 2 -----------------------------------
pages = [
    ("SUPPLY AGREEMENT", [
        ("", "Between BeanCo Roasters (the Supplier) and the Grocery Chain (the Buyer). Effective 1 January 2026."),
        ("1. Scope", "The Supplier supplies ground and whole-bean Coffee to all 20 stores of the Buyer."),
        ("2. Delivery", "Deliveries arrive weekly, on Monday, for orders placed by Wednesday 18:00 the week before."),
        ("3. Price", "Unit cost as per the price schedule in Annex A. Cost changes need 30 days of written notice."),
    ]),
    ("SUPPLY AGREEMENT (continued)", [
        ("4.1 Allocation", "Because of constrained roasting capacity, the Supplier allocates each store at most 70% of that "
                           "store's average weekly volume over the previous quarter. Orders above the allocation are cut back "
                           "to the allocation without notice."),
        ("4.2 Emergency orders", "A store may place one emergency order per week above its allocation, delivered within 48 "
                                 "hours, at a surcharge of 8% on the unit cost, capped at 30% of the weekly allocation."),
        ("7. Review", "Allocation levels are reviewed quarterly. The Buyer may request a higher allocation with 4 weeks of "
                      "sales evidence, including days out of stock."),
    ]),
]
with PdfPages(OUT / "supplier_coffee.pdf") as pdf:
    for page_no, (title, clauses) in enumerate(pages, start=1):
        fig = plt.figure(figsize=(8.27, 11.69))  # A4
        fig.text(0.1, 0.92, title, fontsize=16, weight="bold")
        y = 0.86
        for heading, text in clauses:
            if heading:
                fig.text(0.1, y, heading, fontsize=12, weight="bold")
                y -= 0.03
            words, line = text.split(), ""
            for word in words:  # simple word wrap at ~90 characters
                if len(line) + len(word) > 90:
                    fig.text(0.1, y, line, fontsize=10)
                    y, line = y - 0.022, ""
                line += word + " "
            fig.text(0.1, y, line, fontsize=10)
            y -= 0.05
        fig.text(0.1, 0.05, f"BeanCo Roasters / Grocery Chain - page {page_no} of {len(pages)}", fontsize=8, color="grey")
        pdf.savefig(fig)
        plt.close(fig)

# --- DOCX: area manager visit note for ST-07 (minimal Word document: zipped WordprocessingML) -----------------------
paragraphs = [
    ("Title", "Area manager visit: Store 07 - Westgate"),
    (None, "Visit date: 30 September 2026. Visited by: area manager, North region."),
    ("Heading1", "Ordering practice"),
    (None, "The store manager orders roughly 50% above the system suggestion on every line, to be safe for the weekend. "
           "This is applied to fresh lines too, including sourdough, muffins and strawberries."),
    ("Heading1", "Observations"),
    (None, "Waste bins were full of sourdough and muffins on Monday morning. The back room holds about two weeks of cover on "
           "several dry and plant-based lines. Availability on shelf is good, except coffee, which arrives short every week."),
    ("Heading1", "Agreed actions"),
    (None, "Follow the system order for fresh lines for 4 weeks and review waste. Area manager to check again in October."),
]
def paragraph(style, text):
    props = f'<w:pPr><w:pStyle w:val="{style}"/></w:pPr>' if style else ""
    return f'<w:p>{props}<w:r><w:t xml:space="preserve">{escape(text)}</w:t></w:r></w:p>'


body = "".join(paragraph(style, text) for style, text in paragraphs)
W = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'
styles = "".join(f'<w:style w:type="paragraph" w:styleId="{s}"><w:name w:val="{n}"/><w:rPr><w:b/><w:sz w:val="{z}"/></w:rPr></w:style>'
                 for s, n, z in [("Title", "Title", 36), ("Heading1", "heading 1", 28)])
with zipfile.ZipFile(OUT / "visit_st07_2026_09.docx", "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
               '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
               '<Default Extension="xml" ContentType="application/xml"/>'
               '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
               '<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/></Types>')
    z.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
               '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>')
    z.writestr("word/_rels/document.xml.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
               '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>')
    z.writestr("word/styles.xml", f'<?xml version="1.0" encoding="UTF-8"?><w:styles {W}>{styles}</w:styles>')
    z.writestr("word/document.xml", f'<?xml version="1.0" encoding="UTF-8"?><w:document {W}><w:body>{body}</w:body></w:document>')

# --- PNG: a photographed delivery note (slightly rotated and blurred, so it needs OCR) -------------------------------
img = Image.new("RGB", (1100, 760), "white")
draw = ImageDraw.Draw(img)
try:
    font, bold = ImageFont.truetype("Arial.ttf", 26), ImageFont.truetype("Arial Bold.ttf", 34)
except OSError:
    font = bold = ImageFont.load_default()
lines = [
    (bold, "BeanCo Roasters - DELIVERY NOTE"),
    (font, "Delivery note no. DN-20261026-07"),
    (font, "Deliver to: Store 07 - Westgate (ST-07)"),
    (font, "Delivery date: Monday 26 October 2026"),
    (font, ""),
    (bold, ["Item", "Ordered", "Delivered"]),
    (font, ["Coffee", "264", "96"]),
    (font, ""),
    (font, "Short delivery: 168 units not delivered."),
    (font, "Reason: order above store allocation, cut back"),
    (font, "to allocation as per agreement clause 4.1."),
    (font, ""),
    (font, "Received by: ____________   Signature: ________"),
]
y = 50
for f, text in lines:
    if isinstance(text, list):  # a table row: fixed column positions
        for x, cell in zip((60, 320, 560), text):
            draw.text((x, y), cell, fill="black", font=f)
    else:
        draw.text((60, y), text, fill="black", font=f)
    y += 52 if f is bold else 44
img = img.rotate(1.5, expand=True, fillcolor="#f2f0ea").filter(ImageFilter.GaussianBlur(0.6))
img.save(OUT / "delivery_note_st07_2026_10_26.png")

for path in sorted(OUT.iterdir()):
    print(f"{path.name}: {path.stat().st_size:,} bytes")
