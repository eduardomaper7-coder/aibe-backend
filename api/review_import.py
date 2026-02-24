import os
import re
import tempfile
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pypdf import PdfReader
from openai import OpenAI

from fastapi import APIRouter, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/reviews", tags=["reviews-import"])

DEFAULT_TZ = os.getenv("DEFAULT_TIMEZONE", "Europe/Madrid")
DEFAULT_COUNTRY = os.getenv("DEFAULT_COUNTRY", "ES")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# ---------- Helpers: normalización ----------
PHONE_RE = re.compile(r"[+]?[\d][\d\s().-]{6,}")

def _clean_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = re.sub(r"[^\d+]", "", raw)
    # Si viene sin +, asume ES por defecto (ajústalo si tu producto soporta multi-país)
    if s.startswith("+"):
        return s
    if DEFAULT_COUNTRY == "ES":
        # España: si 9 dígitos y empieza por 6/7/8/9, lo convertimos
        digits = re.sub(r"\D", "", s)
        if len(digits) == 9:
            return "+34" + digits
    # fallback: devuelve solo dígitos si no podemos asegurar E.164
    digits = re.sub(r"\D", "", s)
    return ("+" + digits) if digits else None

def _parse_date_time(value: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (YYYY-MM-DD, HH:MM) si lo puede inferir.
    """
    if value is None or value == "":
        return None, None
    if isinstance(value, datetime):
        return value.date().isoformat(), value.strftime("%H:%M")
    if isinstance(value, date):
        return value.isoformat(), None
    s = str(value).strip()

    # Intento ISO directo
    try:
        dt = datetime.fromisoformat(s.replace("Z", ""))
        return dt.date().isoformat(), dt.strftime("%H:%M")
    except Exception:
        pass

    # Formatos comunes ES
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y", "%d-%m-%Y %H:%M", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            if " %H:%M" in fmt:
                return dt.date().isoformat(), dt.strftime("%H:%M")
            return dt.date().isoformat(), None
        except Exception:
            continue

    return None, None


# ---------- Detección y extracción determinista ----------
def _is_csv(filename: str) -> bool:
    return filename.lower().endswith(".csv")

def _is_excel(filename: str) -> bool:
    return filename.lower().endswith((".xlsx", ".xls"))

def _is_pdf(filename: str) -> bool:
    return filename.lower().endswith(".pdf")

def _is_image(filename: str) -> bool:
    return filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".heic"))


def _extract_from_csv(tmp_path: str) -> List[Dict[str, Any]]:
    df = pd.read_csv(tmp_path)
    return _extract_from_dataframe(df)

def _extract_from_excel(tmp_path: str) -> List[Dict[str, Any]]:
    try:
        # Intento normal
        df = pd.read_excel(tmp_path)
    except Exception:
        # Fallback: leer todas las hojas y unir
        xls = pd.ExcelFile(tmp_path)
        frames = []
        for name in xls.sheet_names:
            try:
                frames.append(pd.read_excel(xls, sheet_name=name))
            except Exception:
                pass

        if not frames:
            return []

        df = pd.concat(frames, ignore_index=True)

    # Limpia columnas vacías
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")

    return _extract_from_dataframe(df)

def _best_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in cols:
            return cols[cand]
    # fuzzy simple
    for c in df.columns:
        cl = str(c).lower()
        if any(k in cl for k in candidates):
            return c
    return None

def _extract_from_dataframe(df: pd.DataFrame) -> List[Dict[str, Any]]:
    # Heurísticas de columnas típicas
    name_col = _best_col(df, ["nombre", "name", "paciente", "patient"])
    phone_col = _best_col(df, ["telefono", "teléfono", "phone", "movil", "móvil", "celular", "mobile"])
    date_col = _best_col(df, ["fecha", "date", "dia", "día"])
    time_col = _best_col(df, ["hora", "time"])
    dt_col = _best_col(df, ["datetime", "fecha_hora", "fecha hora", "start", "inicio"])

    items: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        raw_name = str(row.get(name_col, "")).strip() if name_col else ""
        raw_phone = str(row.get(phone_col, "")).strip() if phone_col else ""

        d, t = None, None
        if dt_col:
            d, t = _parse_date_time(row.get(dt_col))
        else:
            if date_col:
                d, _ = _parse_date_time(row.get(date_col))
            if time_col:
                # hora puede venir como datetime/time o string
                _, t = _parse_date_time(f"2000-01-01 {row.get(time_col)}")

        phone = _clean_phone(raw_phone)

        # Filtra filas vacías
        if not raw_name and not phone and not (d or t):
            continue

        issues = []
        if not raw_name:
            issues.append("missing_name")
        if not phone:
            issues.append("missing_or_invalid_phone")
        if not d:
            issues.append("missing_date")
        if not t:
            issues.append("missing_time")

        items.append({
            "name": raw_name or None,
            "phone": phone,
            "date": d,
            "time": t,
            "timezone": DEFAULT_TZ,
            "notes": None,
            "confidence": 0.95 if len(issues) == 0 else 0.7,
            "issues": issues,
            "source": "deterministic"
        })

    return items


def _extract_text_from_pdf(tmp_path: str) -> str:
    reader = PdfReader(tmp_path)
    texts = []
    for page in reader.pages:
        txt = page.extract_text() or ""
        texts.append(txt)
    return "\n".join(texts).strip()


def _quality_score(items: List[Dict[str, Any]]) -> float:
    if not items:
        return 0.0
    ok_phone = sum(1 for it in items if it.get("phone"))
    ok_time = sum(1 for it in items if it.get("time"))
    ok_date = sum(1 for it in items if it.get("date"))
    n = len(items)
    # score simple
    return (ok_phone/n)*0.45 + (ok_date/n)*0.35 + (ok_time/n)*0.20


# ---------- Extracción con OpenAI (archivos “difíciles”) ----------
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
                        "phone": {"type": ["string", "null"], "description": "Prefer E.164. If missing, null."},
                        "date": {"type": ["string", "null"], "description": "YYYY-MM-DD if known"},
                        "time": {"type": ["string", "null"], "description": "HH:MM 24h if known"},
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

def _openai_extract(file_path: str, filename: str) -> Dict[str, Any]:
    import json
    import re

    # Subimos archivo para usarlo como input
    with open(file_path, "rb") as f:
        uploaded = client.files.create(file=f, purpose="user_data")

    prompt = f"""
Eres un extractor de citas de clínica.
Devuelve SOLO JSON.

Objetivo:
- Extraer una lista de citas con: name, phone, date (YYYY-MM-DD), time (HH:MM 24h).
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

    # 1) Intento A: response_format (algunas versiones)
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

    # 2) Intento B: format (otras versiones)
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

    # 3) Fallback: sin schema (máxima compatibilidad)
    resp = client.responses.create(**base_payload)
    text = resp.output_text or ""

    # extrae el primer bloque JSON que encuentre (por si el modelo añade algo)
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        raise ValueError(f"No se encontró JSON en la respuesta: {text[:300]}")
    return json.loads(m.group(0))

@router.post("/import-appointments")
async def import_appointments(file: UploadFile = File(...)):
    if not os.getenv("OPENAI_API_KEY"):
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY no configurada en backend")

    filename = file.filename or "upload"
    suffix = os.path.splitext(filename)[1].lower()

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name
        content = await file.read()
        tmp.write(content)

    try:
        # CSV -> OpenAI por TEXTO (no se puede como input_file)
        if _is_csv(filename):
            with open(tmp_path, "r", encoding="utf-8", errors="ignore") as f:
                csv_text = f.read()
            data = _openai_extract_from_text(csv_text, filename)
            for it in data.get("appointments", []):
                it["phone"] = _clean_phone(it.get("phone") or "")
                it["timezone"] = it.get("timezone") or DEFAULT_TZ
            return JSONResponse({"mode": "openai_csv_text", **data})

        # Excel -> OpenAI por TEXTO
        if _is_excel(filename):
            df = pd.read_excel(tmp_path)
            excel_text = df.head(300).to_csv(index=False)
            data = _openai_extract_from_text(excel_text, filename)
            for it in data.get("appointments", []):
                it["phone"] = _clean_phone(it.get("phone") or "")
                it["timezone"] = it.get("timezone") or DEFAULT_TZ
            return JSONResponse({"mode": "openai_excel_text", **data})

        # PDF -> OpenAI con archivo (PDF sí soporta input_file)
        if _is_pdf(filename):
            data = _openai_extract(tmp_path, filename)
            for it in data.get("appointments", []):
                it["phone"] = _clean_phone(it.get("phone") or "")
                it["timezone"] = it.get("timezone") or DEFAULT_TZ
            return JSONResponse({"mode": "openai_pdf", **data})

        # ✅ Imagen -> OpenAI como input_image (NO input_file)
        if _is_image(filename):
            data = _openai_extract_image(tmp_path, filename)
            for it in data.get("appointments", []):
                it["phone"] = _clean_phone(it.get("phone") or "")
                it["timezone"] = it.get("timezone") or DEFAULT_TZ
            return JSONResponse({"mode": "openai_image", **data})

        # Otros -> intenta OpenAI por archivo
        data = _openai_extract(tmp_path, filename)
        for it in data.get("appointments", []):
            it["phone"] = _clean_phone(it.get("phone") or "")
            it["timezone"] = it.get("timezone") or DEFAULT_TZ
        return JSONResponse({"mode": "openai_fallback", **data})

    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
def _openai_extract_from_text(text: str, filename: str) -> Dict[str, Any]:
    import json
    import re

    # Limita tamaño para no pasar contexto
    MAX_CHARS = 30000
    if len(text) > MAX_CHARS:
        text = text[:MAX_CHARS]

    prompt = f"""
Eres un extractor automático de citas médicas.

REGLAS:
- Devuelve SOLO JSON válido.
- No escribas explicaciones.
- No uses markdown.
- No añadas texto fuera del JSON.

Objetivo:
- Extraer: name, phone, date (YYYY-MM-DD), time (HH:MM 24h).
- Usa null si falta algo.
- Añade issues: missing_name / missing_phone / missing_date / missing_time.
- Normaliza teléfonos a E.164 si es posible (país {DEFAULT_COUNTRY}).
- timezone: {DEFAULT_TZ}.

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

Contenido del archivo ({filename}):
{text}
""".strip()

    resp = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt}
                ],
            }
        ],
    )

    out = resp.output_text or ""

    # Busca primer JSON válido en la respuesta
    match = re.search(r"\{[\s\S]*\}", out)

    if not match:
        raise ValueError(
            f"No se encontró JSON válido en la respuesta: {out[:300]}"
        )

    return json.loads(match.group(0))

def _openai_extract_image(file_path: str, filename: str) -> Dict[str, Any]:
    import base64, json, re

    with open(file_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")

    prompt = f"""
Eres un extractor de citas de clínica.
Devuelve SOLO JSON válido con este formato:
{{"appointments":[{{"name":null,"phone":null,"date":null,"time":null,"timezone":null,"notes":null,"confidence":0.0,"issues":[]}}],"unparsed":[]}}
Archivo: {filename}
""".strip()

    resp = client.responses.create(
        model="gpt-4.1-mini",
        input=[{
            "role": "user",
            "content": [
                {"type": "input_text", "text": prompt},
                {
                    "type": "input_image",
                    "image_base64": b64,
                },
            ],
        }],
    )

    out = resp.output_text or ""
    m = re.search(r"\{[\s\S]*\}", out)
    if not m:
        raise ValueError(f"No se encontró JSON en la respuesta: {out[:300]}")
    return json.loads(m.group(0))