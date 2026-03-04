from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session
from app.db import get_db
import uuid
from passlib.context import CryptContext
from sqlalchemy import text

router = APIRouter(prefix="/auth", tags=["auth"])
pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")


class SignupIn(BaseModel):
    email: EmailStr
    password: str


class LoginIn(BaseModel):
    email: EmailStr
    password: str
    job_id: int | None = None


class LinkJobIn(BaseModel):
    job_id: int
    email: EmailStr


@router.post("/signup")
def signup(payload: SignupIn, db: Session = Depends(get_db)):
    email = payload.email.lower().strip()
    if len(payload.password) < 6:
        raise HTTPException(400, "Password demasiado corta")

    existing = db.execute(text("select id from users where email=:e"), {"e": email}).fetchone()
    if existing:
        # idempotente: no revienta si ya existe
        return {"ok": True}

    user_id = str(uuid.uuid4())
    ph = pwd.hash(payload.password)

    db.execute(
        text("insert into users (id, email, password_hash) values (:id, :e, :ph)"),
        {"id": user_id, "e": email, "ph": ph},
    )
    db.commit()
    return {"ok": True}


@router.post("/login")
def login(payload: LoginIn, db: Session = Depends(get_db)):
    email = payload.email.lower().strip()

    row = db.execute(
        text("select id, password_hash from users where email=:e"),
        {"e": email},
    ).fetchone()

    if not row:
        raise HTTPException(401, "Invalid credentials")

    user_id, ph = row

    if not pwd.verify(payload.password, ph):
        raise HTTPException(401, "Invalid credentials")

    # Vincular job si viene
    if payload.job_id:
        db.execute(
            text(
                """
                update scrape_jobs
                set user_id=:uid, email=:e
                where id=:jid
                """
            ),
            {"uid": str(user_id), "e": email, "jid": payload.job_id},
        )
        db.commit()

    return {"id": str(user_id), "email": email}


@router.post("/link-job")
def link_job(payload: LinkJobIn, db: Session = Depends(get_db)):
    email = payload.email.lower().strip()
    user = db.execute(text("select id from users where email=:e"), {"e": email}).fetchone()
    if not user:
        raise HTTPException(404, "User not found")

    user_id = str(user[0])

    # vincula el job al user/email
    db.execute(
        text("update scrape_jobs set user_id=:uid, email=:e where id=:job_id"),
        {"uid": user_id, "e": email, "job_id": payload.job_id},
    )
    db.commit()
    return {"ok": True}


@router.get("/job-linked")
def job_linked(job_id: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("select user_id, email from scrape_jobs where id=:jid"),
        {"jid": job_id},
    ).fetchone()

    if not row:
        return {"linked": False}

    user_id, email = row
    return {"linked": bool(user_id or email)}