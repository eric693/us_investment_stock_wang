# -*- coding: utf-8 -*-
"""把 gen_doc.py 產出的 .docx 轉成繁中 PDF（reportlab + 文泉驛正黑，不需 LibreOffice）。"""
import sys
from docx import Document
from docx.document import Document as _Doc
from docx.text.paragraph import Paragraph
from docx.table import Table
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (SimpleDocTemplate, Paragraph as P, Spacer,
                                Table as RTable, TableStyle)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

FONT_PATH = '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc'
pdfmetrics.registerFont(TTFont('CJK', FONT_PATH, subfontIndex=0))
ACCENT = colors.HexColor('#1F4E79')
GREY   = colors.HexColor('#555555')

ss = getSampleStyleSheet()
def st(name, **kw):
    base = dict(fontName='CJK', leading=kw.get('fontSize', 10.5) * 1.5)
    base.update(kw)
    return ParagraphStyle(name, parent=ss['Normal'], **base)

S = {
    'title': st('title', fontSize=22, textColor=ACCENT, spaceAfter=4, leading=28),
    'sub':   st('sub',   fontSize=12, textColor=GREY,   spaceAfter=10),
    'h1':    st('h1',    fontSize=16, textColor=ACCENT, spaceBefore=14, spaceAfter=6, leading=22),
    'h2':    st('h2',    fontSize=12.5, textColor=ACCENT, spaceBefore=8, spaceAfter=4, leading=18),
    'body':  st('body',  fontSize=10.5, spaceAfter=3),
    'bold':  st('bold',  fontSize=10.5, spaceAfter=3),
    'bullet':st('bullet',fontSize=10.5, leftIndent=14, spaceAfter=2),
    'cell':  st('cell',  fontSize=9.5, leading=13),
    'cellk': st('cellk', fontSize=9.5, leading=13, textColor=ACCENT),
}

def esc(t):
    return (t or '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def iter_blocks(doc):
    body = doc.element.body
    for child in body.iterchildren():
        if child.tag.endswith('}p'):
            yield Paragraph(child, doc)
        elif child.tag.endswith('}tbl'):
            yield Table(child, doc)

def para_style(p):
    txt = p.text.strip()
    if not txt:
        return None, None
    runs = p.runs
    size = 10.5; bold = False
    for r in runs:
        if r.font and r.font.size:
            size = r.font.size.pt; break
    bold = any(r.bold for r in runs)
    sname = (p.style.name or '') if p.style else ''
    if 'List Bullet' in sname:
        return 'bullet', '• ' + txt
    if size >= 21:
        return 'title', txt
    if size >= 15:
        return 'h1', txt
    if size >= 12:
        return 'h2', txt
    return ('bold' if bold else 'body'), txt

def build(docx_path, pdf_path):
    doc = Document(docx_path)
    flow = []
    for blk in iter_blocks(doc):
        if isinstance(blk, Paragraph):
            key, txt = para_style(blk)
            if not key:
                flow.append(Spacer(1, 4)); continue
            flow.append(P(esc(txt), S[key]))
        else:  # table
            data = []
            for row in blk.rows:
                cells = row.cells
                if len(cells) >= 2:
                    data.append([P(esc(cells[0].text.strip()), S['cellk']),
                                 P(esc(cells[1].text.strip()), S['cell'])])
                else:
                    data.append([P(esc(cells[0].text.strip()), S['cell']), ''])
            if not data:
                continue
            t = RTable(data, colWidths=[42*mm, 120*mm])
            t.setStyle(TableStyle([
                ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#BBBBBB')),
                ('BACKGROUND', (0,0), (0,-1), colors.HexColor('#DCE6F1')),
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                ('LEFTPADDING', (0,0), (-1,-1), 5),
                ('RIGHTPADDING', (0,0), (-1,-1), 5),
                ('TOPPADDING', (0,0), (-1,-1), 3),
                ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ]))
            flow.append(t); flow.append(Spacer(1, 6))
    SimpleDocTemplate(pdf_path, pagesize=A4,
                      topMargin=18*mm, bottomMargin=16*mm,
                      leftMargin=18*mm, rightMargin=18*mm).build(flow)
    print('PDF SAVED', pdf_path)

if __name__ == '__main__':
    build(sys.argv[1], sys.argv[2])
