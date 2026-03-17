import gzip
import hashlib
import json
import os
import re
import shutil
import tempfile
from typing import Any, Dict, List, Optional


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
    for it in data.get("appointments", []):
        it["phone"] = _clean_phone(it.get("phone") or "")
        it["timezone"] = it.get("timezone") or DEFAULT_TZ
        it["issues"] = list(it.get("issues") or [])
        it["confidence"] = float(it.get("confidence") or 0.0)
    return data




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
                {"type": "input_image", "image_url": data_url},
            ],
        }],
    )


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

    # 2) Intentar parser estructurado primero (CSV / Excel / JSON)
    parsed = _parse_structured_file(tmp_path, filename)
    if parsed and parsed.get("appointments"):
        print(f"⚡ archivo estructurado detectado → sin OpenAI: {filename}")
        return _normalize_extracted_appointments(parsed)

    # 3) Si no tiene extensión, intenta detectar formato estructurado por contenido
    ext = os.path.splitext(filename)[1].lower()
    if not ext:
        # 3A) Intentar como CSV sin extensión
        try:
            df = pd.read_csv(tmp_path)
            if df is not None and not df.empty:
                print(f"📊 archivo sin extensión tratado como CSV: {filename}")

                columns_map = {str(c).lower(): c for c in df.columns}
                phone_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["phone", "movil", "mobile", "tel", "telefono"])]
                name_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["name", "patient", "nombre", "paciente"])]
                date_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["date", "fecha", "dia", "día"])]
                time_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["time", "hora"])]

                appointments = []
                for _, row in df.iterrows():
                    name = row.get(name_cols[0]) if name_cols else None
                    phone = row.get(phone_cols[0]) if phone_cols else None
                    date = row.get(date_cols[0]) if date_cols else None
                    time = row.get(time_cols[0]) if time_cols else None

                    appointments.append({
                        "name": str(name).strip() if name else None,
                        "phone": _clean_phone(str(phone)) if phone else None,
                        "date": str(date).strip() if date else None,
                        "time": str(time).strip() if time else None,
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

        # 3B) Intentar como JSON sin extensión
        try:
            with open(tmp_path, "r", encoding="utf-8") as f:
                raw = json.load(f)

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

                    columns_map = {str(c).lower(): c for c in df.columns}
                    phone_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["phone", "movil", "mobile", "tel", "telefono"])]
                    name_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["name", "patient", "nombre", "paciente"])]
                    date_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["date", "fecha", "dia", "día"])]
                    time_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["time", "hora"])]

                    appointments = []
                    for _, row in df.iterrows():
                        name = row.get(name_cols[0]) if name_cols else None
                        phone = row.get(phone_cols[0]) if phone_cols else None
                        date = row.get(date_cols[0]) if date_cols else None
                        time = row.get(time_cols[0]) if time_cols else None

                        appointments.append({
                            "name": str(name).strip() if name else None,
                            "phone": _clean_phone(str(phone)) if phone else None,
                            "date": str(date).strip() if date else None,
                            "time": str(time).strip() if time else None,
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

    # 4) PDF -> OpenAI
    if _is_pdf(filename):
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
@router.post("/import-appointments", response_model=ImportBatchOut)
async def import_appointments(
    job_id: int = Form(...),
    file: Optional[UploadFile] = File(None),
    files: Optional[List[UploadFile]] = File(None),
    db: Session = Depends(get_db),
):
    print("🔥 import_appointments hit")
    print("🔥 job_id:", job_id)
    print("🔥 file singular:", getattr(file, "filename", None))
    print("🔥 files plural:", [getattr(f, "filename", None) for f in (files or [])])


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
            })
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


    try:
        result = import_appointments_payloads(
            db,
            job_id=job_id,
            files_payload=files_payload,
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




def _parse_structured_file(tmp_path: str, filename: str):

    try:
        if filename.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(tmp_path)

        elif filename.lower().endswith(".csv"):
            df = pd.read_csv(tmp_path)

        elif filename.lower().endswith(".json"):
            with open(tmp_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict):
                if "pacientes" in data and isinstance(data["pacientes"], list):
                    data = data["pacientes"]
                else:
                    data = list(data.values())

            df = pd.DataFrame(data)

        else:
            return None

    except Exception:
        return None

    if df is None or df.empty:
        return None

    columns_map = {str(c).lower(): c for c in df.columns}

    phone_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["phone", "movil", "mobile", "tel", "telefono"])]
    name_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["name", "patient", "nombre", "paciente"])]
    date_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["date", "fecha", "dia", "día"])]
    time_cols = [orig for low, orig in columns_map.items() if any(k in low for k in ["time", "hora"])]

    if not (phone_cols or name_cols or date_cols or time_cols):
        return None

    appointments = []

    for _, row in df.iterrows():

        name = row.get(name_cols[0]) if name_cols else None
        phone = row.get(phone_cols[0]) if phone_cols else None
        date = row.get(date_cols[0]) if date_cols else None
        time = row.get(time_cols[0]) if time_cols else None

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
            "name": str(name).strip() if name else None,
            "phone": _clean_phone(str(phone)) if phone else None,
            "date": str(date).strip() if date else None,
            "time": str(time).strip() if time else None,
            "timezone": DEFAULT_TZ,
            "notes": None,
            "confidence": 1.0,
            "issues": issues,
        })

    return {
        "appointments": appointments,
        "unparsed": []
    }
