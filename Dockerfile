FROM python:3.12-slim

# tesseract-ocr: optional fallback for image-only Connecteam DARs (patrol_automation/ocr_fallback.py)
# poppler-utils: PyMuPDF / pdfplumber image rendering for OCR + PDF-to-image conversions
# fonts-dejavu: branded_pdf.py timeline rendering uses Pillow ImageFont — needs default fonts present
RUN apt-get update && apt-get install -y --no-install-recommends \
        cron curl tesseract-ocr poppler-utils fonts-dejavu \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Fix Windows CRLF line endings and create directories
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh \
    && mkdir -p /app/data /app/pipeline_events \
    && ln -sf /usr/local/bin/python3 /usr/local/bin/python

CMD ["/app/entrypoint.sh"]
