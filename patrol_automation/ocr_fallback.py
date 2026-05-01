"""
OCR fallback for image-only Connecteam PDFs.

Uses Tesseract OCR via pytesseract to extract text from PDF pages that
pdfplumber cannot parse (image-only exports). PyMuPDF renders pages to
images, then pytesseract runs OCR.

Degrades gracefully: if Tesseract is not installed, is_ocr_available()
returns False and no OCR is attempted.
"""

import io
import os
import shutil

_ocr_available = None  # cached after first check


def is_ocr_available():
    """Check if Tesseract OCR is installed and pytesseract is available.
    Caches the result so the filesystem is only probed once per run."""
    global _ocr_available
    if _ocr_available is not None:
        return _ocr_available

    try:
        import pytesseract
    except ImportError:
        _ocr_available = False
        return False

    # Check PATH first
    if shutil.which('tesseract'):
        _ocr_available = True
        return True

    # Check standard Windows install location
    win_path = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    if os.path.isfile(win_path):
        pytesseract.pytesseract.tesseract_cmd = win_path
        _ocr_available = True
        return True

    _ocr_available = False
    return False


def ocr_page(pdf_path, page_num, dpi=150):
    """OCR a single page of a PDF. Returns extracted text or empty string."""
    is_ocr_available()  # ensure tesseract_cmd is configured
    import fitz
    import pytesseract
    from PIL import Image

    doc = fitz.open(str(pdf_path))
    try:
        if page_num >= len(doc):
            return ''
        page = doc[page_num]
        pix = page.get_pixmap(dpi=dpi)
        img = Image.open(io.BytesIO(pix.tobytes('png')))
        text = pytesseract.image_to_string(img, lang='eng')
        return text.strip()
    finally:
        doc.close()


def ocr_pdf_pages(pdf_path, page_numbers=None, dpi=150):
    """OCR multiple pages of a PDF.

    Args:
        pdf_path: Path to the PDF file.
        page_numbers: List of 0-based page numbers to OCR. None = all pages.
        dpi: Resolution for rendering (150 balances speed and accuracy).

    Returns:
        Dict mapping page number to extracted text (only non-empty pages).
    """
    is_ocr_available()  # ensure tesseract_cmd is configured
    import fitz
    import pytesseract
    from PIL import Image

    results = {}
    doc = fitz.open(str(pdf_path))
    try:
        pages = page_numbers if page_numbers is not None else range(len(doc))
        for page_num in pages:
            try:
                page = doc[page_num]
                pix = page.get_pixmap(dpi=dpi)
                img = Image.open(io.BytesIO(pix.tobytes('png')))
                text = pytesseract.image_to_string(img, lang='eng')
                text = text.strip()
                if text:
                    results[page_num] = text
            except Exception:
                continue  # skip failed pages, don't stop the batch
    finally:
        doc.close()

    return results
