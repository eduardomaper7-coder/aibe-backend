import gzip
import hashlib
import json
import os
import re
import shutil
import tempfile
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from pathlib import Path
import mimetypes
from datetime import datetime, timezone, date
import boto3
from botocore.client import Config
from app.review_requests.import_normalizers import normalize_name, normalize_phone
import pandas as pd
from openai import OpenAI
from pypdf import PdfReader


from fastapi import APIRouter, File, UploadFile, HTTPException, Form, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session


from app.db import get_db
from app.review_requests.import_service import import_appointments_payloads
from app.review_requests.import_schemas import ImportBatchOut


router = APIRouter(prefix="/api/reviews", tags=["reviews-import"])


DEFAULT_TZ = os.getenv("DEFAULT_TIMEZONE", "Europe/Madrid")
DEFAULT_COUNTRY = os.getenv("DEFAULT_COUNTRY", "ES")


client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


STORAGE_BUCKET = os.getenv("REVIEW_IMPORTS_BUCKET")
STORAGE_REGION = os.getenv("AWS_REGION", "us-east-1")
STORAGE_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
print("BUCKET:", os.getenv("REVIEW_IMPORTS_BUCKET"))
print("KEY:", os.getenv("AWS_ACCESS_KEY_ID"))
print("ENDPOINT:", os.getenv("S3_ENDPOINT_URL"))

def _get_s3_client():
    if not STORAGE_BUCKET or not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not STORAGE_ENDPOINT_URL:
        raise RuntimeError(
            "Storage no configurado: faltan REVIEW_IMPORTS_BUCKET / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / S3_ENDPOINT_URL"
        )

    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=STORAGE_REGION,
        config=Config(signature_version="s3v4"),
    )


def _safe_filename(filename: str) -> str:
    name = os.path.basename(filename or "upload")
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return name[:180] or "upload"


def _store_original_upload(
    *,
    job_id: int,
    filename: str,
    content: bytes,
    content_type: Optional[str],
    file_hash: str,
) -> dict[str, Any]:
    s3 = _get_s3_client()

    safe_name = _safe_filename(filename)
    now = datetime.now(timezone.utc)
    guessed_type = content_type or mimetypes.guess_type(safe_name)[0] or "application/octet-stream"

    key = (
        f"review-imports/job_{job_id}/"
        f"{now.strftime('%Y/%m/%d/%H%M%S')}_{file_hash[:12]}_{safe_name}"
    )

    s3.put_object(
        Bucket=STORAGE_BUCKET,
        Key=key,
        Body=content,
        ContentType=guessed_type,
    )

    return {
        "storage_provider": "supabase_s3",
        "storage_bucket": STORAGE_BUCKET,
        "storage_key": key,
        "storage_url": None,
        "size_bytes": len(content),
    }

def _clean_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = re.sub(r"[^\d+]", "", raw)
    if s.startswith("+"):
        digits = re.sub(r"\D", "", s[1:])
        return "+" + digits if digits else None


    digits = re.sub(r"\D", "", s)
    if DEFAULT_COUNTRY == "ES" and len(digits) == 9:
        return "+34" + digits
    if 8 <= len(digits) <= 15:
        return "+" + digits
    return None




def _is_csv(filename: str) -> bool:
    return filename.lower().endswith(".csv")




def _is_excel(filename: str) -> bool:
    return filename.lower().endswith((".xlsx", ".xls"))




def _is_pdf(filename: str) -> bool:
    return filename.lower().endswith(".pdf")




def _is_image(filename: str) -> bool:
    return filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".heic"))


def _is_gz(filename: str) -> bool:
    return filename.lower().endswith(".gz")


def _gunzip_file(tmp_path: str, filename: str) -> tuple[str, str]:
    """
    Descomprime un .gz a un archivo temporal y devuelve:
    (ruta_descomprimida, nombre_archivo_interno)
    """
    original_name = filename[:-3] if filename.lower().endswith(".gz") else filename
    inner_suffix = os.path.splitext(original_name)[1]

    with tempfile.NamedTemporaryFile(delete=False, suffix=inner_suffix or "") as out:
        out_path = out.name

    with gzip.open(tmp_path, "rb") as gz_in, open(out_path, "wb") as f_out:
        shutil.copyfileobj(gz_in, f_out)

    print(f"📦 gunzip completado: {filename} -> {original_name} ({out_path})")
    return out_path, original_name


def _try_read_text_file(tmp_path: str) -> Optional[str]:
    """
    Intenta leer un archivo como texto UTF-8 / latin-1.
    Devuelve el texto si parece legible; si no, None.
    """
    for enc in ("utf-8", "latin-1"):
        try:
            with open(tmp_path, "r", encoding=enc, errors="strict") as f:
                text = f.read()


            if not text:
                return ""


            printable = sum(1 for ch in text if ch.isprintable() or ch in "\n\r\t")
            ratio = printable / max(len(text), 1)


            if ratio >= 0.85:
                return text
        except Exception:
            continue


    return None
def _extract_pdf_text(file_path: str) -> Optional[str]:
    """
    Extrae texto plano de un PDF usando pypdf.
    Devuelve None si no puede extraer nada útil.
    """
    try:
        reader = PdfReader(file_path)
        pages_text = []

        for page in reader.pages:
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""

            if text and text.strip():
                pages_text.append(text)

        full_text = "\n".join(pages_text).strip()
        return full_text or None
    except Exception:
        return None


def _normalize_detected_date(raw: str) -> Optional[str]:
    if not raw:
        return None

    raw = raw.strip()

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y", "%d-%m-%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    try:
        dt = pd.to_datetime(raw, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _normalize_detected_time(raw: str) -> Optional[str]:
    if not raw:
        return None

    raw = raw.strip().replace(".", ":")

    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).strftime("%H:%M")
        except Exception:
            pass

    m = re.match(r"^(\d{1,2}):(\d{2})$", raw)
    if not m:
        return None

    hh = int(m.group(1))
    mm = int(m.group(2))
    if 0 <= hh <= 23 and 0 <= mm <= 59:
        return f"{hh:02d}:{mm:02d}"

    return None

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

SPANISH_WEEKDAYS = {
    "lunes", "martes", "miercoles", "miércoles", "jueves",
    "viernes", "sabado", "sábado", "domingo"
}


def _normalize_text_for_date(value: str) -> str:
    s = (value or "").strip().lower()
    s = re.sub(r"[,\.;]", " ", s)
    s = re.sub(r"\bde\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_spanish_text_date(raw: str) -> Optional[str]:
    if not raw:
        return None

    s = _normalize_text_for_date(raw)
    if not s:
        return None

    tokens = [tok for tok in s.split() if tok not in SPANISH_WEEKDAYS]
    if not tokens:
        return None

    s = " ".join(tokens)
    current_year = datetime.now().year

    # 14 abril 2025 / 14 abril
    m = re.match(r"^(\d{1,2})\s+([a-záéíóú]+)(?:\s+(\d{4}))?$", s, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3)) if m.group(3) else current_year
        month = SPANISH_MONTHS.get(month_name)
        if month:
            try:
                return date(year, month, day).isoformat()
            except Exception:
                return None

    # abril 14 2025 / abril 14
    m = re.match(r"^([a-záéíóú]+)\s+(\d{1,2})(?:\s+(\d{4}))?$", s, re.IGNORECASE)
    if m:
        month_name = m.group(1).lower()
        day = int(m.group(2))
        year = int(m.group(3)) if m.group(3) else current_year
        month = SPANISH_MONTHS.get(month_name)
        if month:
            try:
                return date(year, month, day).isoformat()
            except Exception:
                return None

    return None


def _repair_extracted_appointments(data: Dict[str, Any]) -> Dict[str, Any]:
    appointments = data.get("appointments") or []
    if not appointments:
        return data

    # Normalizar horas y fechas individuales
    for it in appointments:
        raw_time = it.get("time")
        raw_date = it.get("date")

        norm_time = _normalize_detected_time(raw_time or "")
        if norm_time:
            it["time"] = norm_time

        norm_date = _normalize_detected_date(raw_date or "")
        if not norm_date:
            norm_date = _parse_spanish_text_date(raw_date or "")
        if norm_date:
            it["date"] = norm_date

    # Si hay una fecha dominante/no nula, propagarla a las citas sin fecha
    unique_dates = []
    for it in appointments:
        d = it.get("date")
        if d and d not in unique_dates:
            unique_dates.append(d)

    if len(unique_dates) == 1:
        shared_date = unique_dates[0]
        for it in appointments:
            if not it.get("date"):
                it["date"] = shared_date
                issues = list(it.get("issues") or [])
                issues = [x for x in issues if x != "missing_date"]
                it["issues"] = issues

    # Recalcular issues mínimos
    for it in appointments:
        issues = list(it.get("issues") or [])

        if not (it.get("name") or "").strip():
            if "missing_name" not in issues:
                issues.append("missing_name")
        else:
            issues = [x for x in issues if x != "missing_name"]

        if not (it.get("phone") or "").strip():
            if "missing_phone" not in issues:
                issues.append("missing_phone")
        else:
            issues = [x for x in issues if x != "missing_phone"]

        if not (it.get("date") or "").strip():
            if "missing_date" not in issues:
                issues.append("missing_date")
        else:
            issues = [x for x in issues if x != "missing_date"]

        if not (it.get("time") or "").strip():
            if "missing_time" not in issues:
                issues.append("missing_time")
        else:
            issues = [x for x in issues if x != "missing_time"]

        it["issues"] = issues

    data["appointments"] = appointments
    return data


def _extract_appointments_from_text_locally(text: str) -> Optional[Dict[str, Any]]:
    """
    Heurística local para texto extraído de PDFs.
    Intenta detectar bloques con nombre, teléfono, fecha y hora.
    """
    if not text or not text.strip():
        return None

    blocks = re.split(r"\n\s*\n+", text)
    appointments = []

    phone_re = re.compile(r"(\+?\d[\d\s().-]{7,}\d)")
    time_re = re.compile(r"\b(\d{1,2}:\d{2})\b")

    date_patterns = [
        re.compile(r"\b(\d{4}-\d{2}-\d{2})\b"),
        re.compile(r"\b(\d{2}/\d{2}/\d{4})\b"),
        re.compile(r"\b(\d{2}-\d{2}-\d{4})\b"),
        re.compile(r"\b(\d{2}/\d{2}/\d{2})\b"),
        re.compile(r"\b(\d{2}-\d{2}-\d{2})\b"),
    ]

    for block in blocks:
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue

        joined = " | ".join(lines)

        phone_match = phone_re.search(joined)
        raw_phone = phone_match.group(1) if phone_match else None
        phone = _clean_phone(raw_phone or "")

        raw_date = None
        for date_re in date_patterns:
            m = date_re.search(joined)
            if m:
                raw_date = m.group(1)
                break
        date = _normalize_detected_date(raw_date or "")

        time_match = time_re.search(joined)
        raw_time = time_match.group(1) if time_match else None
        time = _normalize_detected_time(raw_time or "")

        # Nombre: primera línea "útil" que no sea claramente fecha/hora/teléfono
        name = None
        for line in lines:
            normalized = line.lower()

            if phone_re.search(line):
                continue
            if time_re.search(line):
                continue
            if any(dp.search(line) for dp in date_patterns):
                continue
            if normalized in {"calendar", "google calendar", "viernes", "sábado", "domingo"}:
                continue

            if len(line) >= 2:
                name = line.strip()
                break

        # Si no detectamos nada relevante, ignoramos el bloque
        if not any([name, phone, date, time]):
            continue

        issues = []
        if not name:
            issues.append("missing_name")
        if not phone:
            issues.append("missing_phone")
        if not date:
            issues.append("missing_date")
        if not time:
            issues.append("missing_time")

        appointments.append({
            "name": name,
            "phone": phone,
            "date": date,
            "time": time,
            "timezone": DEFAULT_TZ,
            "notes": None,
            "confidence": 0.7,
            "issues": issues,
        })

    if not appointments:
        return None

    return {
        "appointments": appointments,
        "unparsed": [],
    }

JSON_SCHEMA: Dict[str, Any] = {
    "name": "appointments_extract",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "appointments": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "name": {"type": ["string", "null"]},
                        "phone": {"type": ["string", "null"]},
                        "date": {"type": ["string", "null"]},
                        "time": {"type": ["string", "null"]},
                        "timezone": {"type": ["string", "null"]},
                        "notes": {"type": ["string", "null"]},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "issues": {"type": "array", "items": {"type": "string"}}
                    },
                    "required": ["name", "phone", "date", "time", "timezone", "notes", "confidence", "issues"]
                }
            },
            "unparsed": {"type": "array", "items": {"type": "string"}}
        },
        "required": ["appointments", "unparsed"]
    }
}




def _normalize_extracted_appointments(data: Dict[str, Any]) -> Dict[str, Any]:
    data = _repair_extracted_appointments(data)

    for it in data.get("appointments", []):
        raw_phone = it.get("phone") or ""
        raw_date = it.get("date") or ""
        raw_time = it.get("time") or ""

        it["phone"] = _clean_phone(raw_phone)
        it["date"] = (
            _normalize_detected_date(raw_date)
            or _parse_spanish_text_date(raw_date)
            or (raw_date.strip() if isinstance(raw_date, str) and raw_date.strip() else None)
        )
        it["time"] = _normalize_detected_time(raw_time) or (
            raw_time.strip() if isinstance(raw_time, str) and raw_time.strip() else None
        )
        it["timezone"] = it.get("timezone") or DEFAULT_TZ
        it["issues"] = list(it.get("issues") or [])
        it["confidence"] = float(it.get("confidence") or 0.0)

        if not it.get("name") and "missing_name" not in it["issues"]:
            it["issues"].append("missing_name")
        if not it.get("phone") and "missing_phone" not in it["issues"]:
            it["issues"].append("missing_phone")
        if not it.get("date") and "missing_date" not in it["issues"]:
            it["issues"].append("missing_date")
        if not it.get("time") and "missing_time" not in it["issues"]:
            it["issues"].append("missing_time")

    return _repair_extracted_appointments(data)




def _openai_extract(file_path: str, filename: str) -> Dict[str, Any]:
    import json
    import re


    with open(file_path, "rb") as f:
        uploaded = client.files.create(file=f, purpose="user_data")


    prompt = f"""
Eres un extractor de citas de clínica.
Devuelve SOLO JSON.


Objetivo:
- Extraer una lista de fragmentos de citas con: name, phone, date (YYYY-MM-DD), time (HH:MM 24h).
- Si falta un dato, usa null y añade un issue: missing_name / missing_phone / missing_date / missing_time.
- Normaliza teléfonos a E.164 si es posible (por defecto país {DEFAULT_COUNTRY}).
- timezone por defecto: {DEFAULT_TZ}.


Formato JSON esperado:
{{
  "appointments": [
    {{
      "name": "string|null",
      "phone": "string|null",
      "date": "YYYY-MM-DD|null",
      "time": "HH:MM|null",
      "timezone": "string|null",
      "notes": "string|null",
      "confidence": 0.0,
      "issues": ["..."]
    }}
  ],
  "unparsed": ["..."]
}}


Archivo: {filename}
""".strip()


    base_payload = dict(
        model="gpt-4.1-mini",
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_file", "file_id": uploaded.id},
                ],
            }
        ],
    )


    try:
        resp = client.responses.create(
            **base_payload,
            response_format={
                "type": "json_schema",
                "json_schema": JSON_SCHEMA,
            },
        )
        return json.loads(resp.output_text)
    except TypeError:
        pass


    try:
        resp = client.responses.create(
            **base_payload,
            format={
                "type": "json_schema",
                "json_schema": JSON_SCHEMA,
            },
        )
        return json.loads(resp.output_text)
    except TypeError:
        pass


    resp = client.responses.create(**base_payload)
    text = resp.output_text or ""
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        raise ValueError(f"No se encontró JSON en la respuesta: {text[:300]}")
    return json.loads(m.group(0))




def _openai_extract_from_text(text: str, filename: str) -> Dict[str, Any]:


    chunks = _chunk_text(text, 25000)


    all_appointments = []
    all_unparsed = []


    for chunk in chunks:


        prompt = f"""
Eres un extractor automático de citas médicas.


Devuelve SOLO JSON válido.


Formato obligatorio:


{{
  "appointments": [
    {{
      "name": null,
      "phone": null,
      "date": null,
      "time": null,
      "timezone": null,
      "notes": null,
      "confidence": 0.0,
      "issues": []
    }}
  ],
  "unparsed": []
}}


Objetivo:
- Extraer citas médicas con nombre, teléfono, fecha y hora.
- Usa null si falta algún dato.
- Añade issues: missing_name / missing_phone / missing_date / missing_time.
- Normaliza teléfonos a E.164 si es posible (país {DEFAULT_COUNTRY}).
- timezone por defecto: {DEFAULT_TZ}.


Contenido del archivo ({filename}):


{chunk}
""".strip()


        resp = client.responses.create(
            model="gpt-4.1-mini",
            input=[{
                "role": "user",
                "content": [{"type": "input_text", "text": prompt}],
            }],
        )


        out = resp.output_text or ""
        m = re.search(r"\{[\s\S]*\}", out)


        if not m:
            continue


        data = json.loads(m.group(0))


        all_appointments.extend(data.get("appointments", []))
        all_unparsed.extend(data.get("unparsed", []))


    return {
        "appointments": all_appointments,
        "unparsed": all_unparsed
    }




def _openai_extract_image(file_path: str, filename: str) -> Dict[str, Any]:
    import base64
    import json
    import re
    from datetime import datetime

    ext = (filename or "").lower().split(".")[-1]
    mime = {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "webp": "image/webp",
        "heic": "image/heic",
    }.get(ext, "image/png")

    with open(file_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")

    data_url = f"data:{mime};base64,{b64}"
    current_year = datetime.now().year

    prompt = f"""
Eres un extractor experto de citas médicas a partir de imágenes.
La imagen puede ser una agenda manuscrita, una hoja impresa, una captura, una foto con texto a mano o una mezcla de ambas.

Devuelve SOLO JSON válido con este formato exacto:
{{
  "appointments": [
    {{
      "name": null,
      "phone": null,
      "date": null,
      "time": null,
      "timezone": null,
      "notes": null,
      "confidence": 0.0,
      "issues": []
    }}
  ],
  "unparsed": []
}}

Objetivo:
- Detectar todas las citas visibles.
- Cada cita debe tener name, phone, date, time.
- Si falta un dato, usa null y añade: missing_name / missing_phone / missing_date / missing_time.
- timezone por defecto: {DEFAULT_TZ}.
- Normaliza teléfonos españoles de 9 dígitos a E.164 (+34...).
- Las horas pueden venir como 9.40, 9:40, 930, 13.15. Devuélvelas como HH:MM.
- Si la fecha está una sola vez en el encabezado de la agenda, aplícala a todas las citas de esa página.
- La fecha puede venir como:
  - 14/04/2025
  - 14-04-2025
  - 2025-04-14
  - 14 abril 2025
  - abril 14 2025
  - lunes 14 abril
  - abril 14 lunes
- Si aparece día y mes pero no año, usa {current_year}, salvo que el documento muestre claramente otro año.
- No inventes nombres ni teléfonos.
- Ignora líneas vacías, adornos, separadores, horas sin paciente y marcas visuales.
- Si una línea tiene estructura tipo: hora + nombre + teléfono, interprétala como cita.
- Si una misma fecha general aplica a toda la hoja, no marques missing_date por cada fila si la fecha puede inferirse claramente del encabezado.
- Si hay texto dudoso, ponlo en notes o en unparsed, pero no inventes.

Archivo: {filename}
""".strip()

    base_payload = dict(
        model="gpt-4.1",
        input=[{
            "role": "user",
            "content": [
                {"type": "input_text", "text": prompt},
                {"type": "input_image", "image_url": data_url},
            ],
        }],
    )

    try:
        resp = client.responses.create(
            **base_payload,
            response_format={
                "type": "json_schema",
                "json_schema": JSON_SCHEMA,
            },
        )
        return json.loads(resp.output_text)
    except TypeError:
        pass

    try:
        resp = client.responses.create(
            **base_payload,
            format={
                "type": "json_schema",
                "json_schema": JSON_SCHEMA,
            },
        )
        return json.loads(resp.output_text)
    except TypeError:
        pass

    resp = client.responses.create(**base_payload)
    out = resp.output_text or ""
    m = re.search(r"\{[\s\S]*\}", out)
    if not m:
        raise ValueError(f"No se encontró JSON en la respuesta: {out[:300]}")
    return json.loads(m.group(0))

def _extract_with_openai(tmp_path: str, filename: str) -> Dict[str, Any]:
    # 1) .gz -> descomprimir y volver a procesar
    if _is_gz(filename):
        extracted_path = None
        try:
            extracted_path, inner_filename = _gunzip_file(tmp_path, filename)
            print(f"📦 .gz detectado: {filename} -> archivo interno: {inner_filename}")
            return _extract_with_openai(extracted_path, inner_filename)
        finally:
            if extracted_path:
                try:
                    os.unlink(extracted_path)
                except Exception:
                    pass

    # 2) Intentar parser estructurado primero (CSV / Excel / JSON con extensión)
    parsed = _parse_structured_file(tmp_path, filename)
    if parsed and parsed.get("appointments"):
        print(f"⚡ archivo estructurado detectado → sin OpenAI: {filename}")
        return _normalize_extracted_appointments(parsed)

    # 3) Si no tiene extensión, detectar formato por cabecera
    ext = os.path.splitext(filename)[1].lower()
    if not ext:
        head = b""
        try:
            with open(tmp_path, "rb") as f:
                head = f.read(4096).lstrip()
        except Exception:
            head = b""

        looks_like_json = head.startswith(b"{") or head.startswith(b"[")
        looks_like_csv = (
            b"," in head[:1024]
            or b";" in head[:1024]
            or b"\t" in head[:1024]
        )

        # 3A) JSON primero si parece JSON
        if looks_like_json:
            try:
                with open(tmp_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)

                nested = _parse_nested_patients_json(raw) if isinstance(raw, dict) else None
                if nested:
                    print(f"🧾 archivo sin extensión tratado como JSON anidado: {filename}")
                    return _normalize_extracted_appointments(nested)

                if isinstance(raw, dict):
                    if "pacientes" in raw and isinstance(raw["pacientes"], list):
                        data = raw["pacientes"]
                    else:
                        data = list(raw.values())
                elif isinstance(raw, list):
                    data = raw
                else:
                    data = []

                if data:
                    df = pd.DataFrame(data)
                    if not df.empty:
                        print(f"🧾 archivo sin extensión tratado como JSON: {filename}")

                        columns_map = {str(c).strip().lower(): c for c in df.columns}
                        phone_cols = [
                            orig for low, orig in columns_map.items()
                            if any(k in low for k in ["phone", "movil", "móvil", "mobile", "tel", "telefono", "teléfono"])
                        ]
                        name_cols = [
                            orig for low, orig in columns_map.items()
                            if any(k in low for k in ["name", "patient", "nombre", "paciente", "cliente"])
                        ]
                        date_cols = [
                            orig for low, orig in columns_map.items()
                            if any(k in low for k in ["date", "fecha", "dia", "día"])
                        ]
                        time_cols = [
                            orig for low, orig in columns_map.items()
                            if any(k in low for k in ["time", "hora", "inicio"])
                        ]

                        appointments = []
                        for _, row in df.iterrows():
                            name = row.get(name_cols[0]) if name_cols else None
                            phone = row.get(phone_cols[0]) if phone_cols else None
                            date = row.get(date_cols[0]) if date_cols else None
                            time = row.get(time_cols[0]) if time_cols else None

                            appointments.append({
                                "name": str(name).strip() if name is not None and str(name).strip() else None,
                                "phone": _clean_phone(str(phone)) if phone is not None and str(phone).strip() else None,
                                "date": str(date).strip() if date is not None and str(date).strip() else None,
                                "time": str(time).strip()[:5] if time is not None and str(time).strip() else None,
                                "timezone": DEFAULT_TZ,
                                "notes": None,
                                "confidence": 1.0,
                                "issues": [],
                            })

                        return _normalize_extracted_appointments({
                            "appointments": appointments,
                            "unparsed": []
                        })
            except Exception:
                pass

        # 3B) CSV solo si parece CSV o si JSON no funcionó
        if looks_like_csv or not looks_like_json:
            try:
                df = pd.read_csv(tmp_path)
                if df is not None and not df.empty:
                    print(f"📊 archivo sin extensión tratado como CSV: {filename}")

                    columns_map = {str(c).strip().lower(): c for c in df.columns}
                    phone_cols = [
                        orig for low, orig in columns_map.items()
                        if any(k in low for k in ["phone", "movil", "móvil", "mobile", "tel", "telefono", "teléfono"])
                    ]
                    name_cols = [
                        orig for low, orig in columns_map.items()
                        if any(k in low for k in ["name", "patient", "nombre", "paciente", "cliente"])
                    ]
                    date_cols = [
                        orig for low, orig in columns_map.items()
                        if any(k in low for k in ["date", "fecha", "dia", "día"])
                    ]
                    time_cols = [
                        orig for low, orig in columns_map.items()
                        if any(k in low for k in ["time", "hora", "inicio"])
                    ]

                    appointments = []
                    for _, row in df.iterrows():
                        name = row.get(name_cols[0]) if name_cols else None
                        phone = row.get(phone_cols[0]) if phone_cols else None
                        date = row.get(date_cols[0]) if date_cols else None
                        time = row.get(time_cols[0]) if time_cols else None

                        appointments.append({
                            "name": str(name).strip() if name is not None and str(name).strip() else None,
                            "phone": _clean_phone(str(phone)) if phone is not None and str(phone).strip() else None,
                            "date": str(date).strip() if date is not None and str(date).strip() else None,
                            "time": str(time).strip()[:5] if time is not None and str(time).strip() else None,
                            "timezone": DEFAULT_TZ,
                            "notes": None,
                            "confidence": 1.0,
                            "issues": [],
                        })

                    return _normalize_extracted_appointments({
                        "appointments": appointments,
                        "unparsed": []
                    })
            except Exception:
                pass

        # 3C) Si no era estructurado, leer como texto y mandar a OpenAI
        text = _try_read_text_file(tmp_path)
        if text is not None:
            print(f"📄 archivo sin extensión tratado como texto: {filename}")
            return _normalize_extracted_appointments(
                _openai_extract_from_text(text, filename)
            )

    # 4) PDF -> OpenAI directo
    if _is_pdf(filename):
        print(f"📄 PDF enviado a OpenAI directamente: {filename}")
        return _normalize_extracted_appointments(
            _openai_extract(tmp_path, filename)
        )

    # 5) Imagen -> OpenAI
    if _is_image(filename):
        return _normalize_extracted_appointments(
            _openai_extract_image(tmp_path, filename)
        )

    # 6) Fallback final -> OpenAI
    return _normalize_extracted_appointments(
        _openai_extract(tmp_path, filename)
    )

def _collect_import_keys(files_payload: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    phones: set[str] = set()
    names: set[str] = set()

    for file_payload in files_payload:
        for raw in file_payload.get("appointments") or []:
            phone = normalize_phone(raw.get("phone"))
            name = normalize_name(raw.get("name"))

            if phone:
                phones.add(phone)
            if name:
                names.add(name)

    return phones, names


@router.post("/import-appointments", response_model=ImportBatchOut)
async def import_appointments(
    job_id: int = Form(...),
    file: Optional[UploadFile] = File(None),
    files: Optional[List[UploadFile]] = File(None),
    db: Session = Depends(get_db),
):
    print("🔥 import_appointments hit", job_id)

    if not os.getenv("OPENAI_API_KEY"):
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY no configurada en backend")

    incoming_files: List[UploadFile] = []

    if file is not None:
        incoming_files.append(file)

    if files:
        incoming_files.extend(files)

    if not incoming_files:
        raise HTTPException(status_code=400, detail="Debes subir al menos un archivo")

    files_payload: list[dict[str, Any]] = []

    for current_file in incoming_files:
        filename = current_file.filename or "upload"
        suffix = os.path.splitext(filename)[1].lower()

        content = await current_file.read()
        file_hash = hashlib.sha256(content).hexdigest()

        stored = _store_original_upload(
            job_id=job_id,
            filename=filename,
            content=content,
            content_type=current_file.content_type,
            file_hash=file_hash,
        )

        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name
            tmp.write(content)

        try:
            data = _extract_with_openai(tmp_path, filename)
            files_payload.append({
                "original_filename": filename,
                "mime_type": current_file.content_type,
                "file_hash": file_hash,
                "appointments": data.get("appointments", []),
                "storage_provider": stored["storage_provider"],
                "storage_bucket": stored["storage_bucket"],
                "storage_key": stored["storage_key"],
                "storage_url": stored.get("storage_url"),
                "size_bytes": stored["size_bytes"],
            })
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    try:
        # 🔥 NUEVO: extraer claves para precarga eficiente
        preload_phones, preload_names = _collect_import_keys(files_payload)

        result = import_appointments_payloads(
            db,
            job_id=job_id,
            files_payload=files_payload,
            preload_phones=preload_phones,
            preload_names=preload_names,
        )
        return JSONResponse(result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No se pudo importar: {e}")

def _chunk_text(text: str, size: int = 25000):
    chunks = []
    start = 0


    while start < len(text):
        chunks.append(text[start:start+size])
        start += size


    return chunks




def _pick_first_matching_column(columns_map: dict[str, str], keywords: list[str]) -> Optional[str]:
    for low, original in columns_map.items():
        if any(k in low for k in keywords):
            return original
    return None


def _parse_nested_patients_json(data: dict) -> Optional[Dict[str, Any]]:
    pacientes = data.get("pacientes")
    if not isinstance(pacientes, list):
        return None

    appointments = []

    for p in pacientes:
        nombre = (p.get("nombre") or "").strip()
        apellidos = (p.get("apellidos") or "").strip()

        full_name = " ".join(
            part for part in [nombre, apellidos] if part
        ).strip() or None

        phone_raw = (
            p.get("movil")
            or p.get("mobile")
            or p.get("telefono")
            or p.get("teléfono")
            or p.get("phone")
        )

        phone = _clean_phone(str(phone_raw).strip()) if phone_raw else None

        procesos = p.get("procesos") or []

        if not procesos:
            issues = []
            if not full_name:
                issues.append("missing_name")
            if not phone:
                issues.append("missing_phone")
            issues.extend(["missing_date", "missing_time"])

            appointments.append({
                "name": full_name,
                "phone": phone,
                "date": None,
                "time": None,
                "timezone": DEFAULT_TZ,
                "notes": None,
                "confidence": 1.0,
                "issues": issues,
            })
            continue

        for proceso in procesos:
            citas = proceso.get("citas") or []

            if not citas:
                issues = []
                if not full_name:
                    issues.append("missing_name")
                if not phone:
                    issues.append("missing_phone")
                issues.extend(["missing_date", "missing_time"])

                appointments.append({
                    "name": full_name,
                    "phone": phone,
                    "date": None,
                    "time": None,
                    "timezone": DEFAULT_TZ,
                    "notes": None,
                    "confidence": 1.0,
                    "issues": issues,
                })
                continue

            for cita in citas:
                raw_date = cita.get("fecha")
                raw_time = cita.get("inicio") or cita.get("hora") or cita.get("time")

                issues = []
                if not full_name:
                    issues.append("missing_name")
                if not phone:
                    issues.append("missing_phone")
                if not raw_date:
                    issues.append("missing_date")
                if not raw_time:
                    issues.append("missing_time")

                appointments.append({
                    "name": full_name,
                    "phone": phone,
                    "date": str(raw_date).strip() if raw_date else None,
                    "time": str(raw_time).strip()[:5] if raw_time else None,
                    "timezone": DEFAULT_TZ,
                    "notes": None,
                    "confidence": 1.0,
                    "issues": issues,
                })

    return {
        "appointments": appointments,
        "unparsed": [],
    }

def _parse_structured_file(tmp_path: str, filename: str):
    try:
        if filename.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(tmp_path)

        elif filename.lower().endswith(".csv"):
            df = pd.read_csv(tmp_path)

        elif filename.lower().endswith(".json"):
            with open(tmp_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            nested = _parse_nested_patients_json(data) if isinstance(data, dict) else None
            if nested:
                return nested

            if isinstance(data, dict):
                data = list(data.values())
            elif not isinstance(data, list):
                return None

            df = pd.DataFrame(data)

        else:
            return None

    except Exception:
        return None

    if df is None or df.empty:
        return None

    columns_map = {str(c).strip().lower(): c for c in df.columns}

    name_col = _pick_first_matching_column(columns_map, [
        "nombre", "name", "paciente", "patient", "cliente"
    ])

    phone_col = _pick_first_matching_column(columns_map, [
        "telefono", "teléfono", "movil", "móvil", "mobile", "phone", "tel", "telf"
    ])

    date_col = _pick_first_matching_column(columns_map, [
        "fecha", "date", "dia", "día", "fecha cita"
    ])

    time_col = _pick_first_matching_column(columns_map, [
        "hora", "time", "inicio cita", "inicio", "hora cita", "comienzo"
    ])

    # OJO: quitamos "inicio cita" de aquí
    datetime_col = _pick_first_matching_column(columns_map, [
        "fecha hora", "fecha_hora", "datetime", "start"
    ])

    print("🧩 columnas detectadas:", {
        "name_col": name_col,
        "phone_col": phone_col,
        "date_col": date_col,
        "time_col": time_col,
        "datetime_col": datetime_col,
    })

    if not any([name_col, phone_col, date_col, time_col, datetime_col]):
        return None

    appointments = []

    for _, row in df.iterrows():
        raw_name = row.get(name_col) if name_col else None
        raw_phone = row.get(phone_col) if phone_col else None

        raw_date = None
        raw_time = None

        # Prioridad: si hay fecha/hora separadas, usar eso
        if date_col or time_col:
            raw_date = row.get(date_col) if date_col else None
            raw_time = row.get(time_col) if time_col else None

        # Solo usar datetime combinado si no existen columnas separadas
        elif datetime_col:
            dt_value = row.get(datetime_col)
            if dt_value is not None and str(dt_value).strip():
                if hasattr(dt_value, "strftime"):
                    raw_date = dt_value.strftime("%Y-%m-%d")
                    raw_time = dt_value.strftime("%H:%M")
                else:
                    dt_text = str(dt_value).strip()
                    try:
                        parsed = pd.to_datetime(dt_text, errors="raise")
                        raw_date = parsed.strftime("%Y-%m-%d")
                        raw_time = parsed.strftime("%H:%M")
                    except Exception:
                        raw_date = None
                        raw_time = None

        raw_name_str = str(raw_name).strip() if raw_name is not None and str(raw_name).strip() else None
        raw_phone_str = str(raw_phone).strip() if raw_phone is not None and str(raw_phone).strip() else None
        raw_date_str = str(raw_date).strip() if raw_date is not None and str(raw_date).strip() else None
        raw_time_str = str(raw_time).strip() if raw_time is not None and str(raw_time).strip() else None

        issues = []
        if not raw_name_str:
            issues.append("missing_name")
        if not raw_phone_str:
            issues.append("missing_phone")
        if not raw_date_str:
            issues.append("missing_date")
        if not raw_time_str:
            issues.append("missing_time")

        appointments.append({
            "name": raw_name_str,
            "phone": _clean_phone(raw_phone_str) if raw_phone_str else None,
            "date": raw_date_str,
            "time": raw_time_str[:5] if raw_time_str else None,
            "timezone": DEFAULT_TZ,
            "notes": None,
            "confidence": 1.0,
            "issues": issues,
        })

    return {
        "appointments": appointments,
        "unparsed": []
    }