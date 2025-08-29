#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
╔══════════════════════════════════════════════════════════════════════╗
║  IMAP → MinIO (WAV) → OpenAI Transcribe → Webhook POST               ║
║                                                                      ║
║  • Pollt IMAP auf UNSEEN-Mails                                       ║
║  • Filtert auf erlaubte Absender                                     ║
║  • Extrahiert Betreff & Text                                         ║
║  • Lädt WAV-Anhänge flat in MinIO hoch                               ║
║  • Transkribiert WAV mit OpenAI                                      ║
║  • POSTet JSON an WEBHOOK_URL (ohne Transkript in MinIO zu speichern)║
║                                                                      ║
║  Konfiguration via .env                                              ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import os
import re
import io
import json
import time
import email
import tempfile
import logging
from typing import List, Tuple
from datetime import datetime, timezone
from email.message import Message
from urllib.parse import urlparse
import subprocess

import requests
from dotenv import load_dotenv
from imapclient import IMAPClient

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

from openai import OpenAI

# ──────────────────────────── Setup / Config ────────────────────────────

load_dotenv()

# Logging: klar, prägnant, mit Zeitstempel und Level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("imap-minio-webhook")

# IMAP
IMAP_HOST   = os.getenv("IMAP_HOST")
IMAP_PORT   = int(os.getenv("IMAP_PORT", "993"))
IMAP_USER   = os.getenv("IMAP_USER")
IMAP_PASS   = os.getenv("IMAP_PASS")
IMAP_FOLDER = os.getenv("IMAP_FOLDER", "INBOX")
INTERVAL    = int(os.getenv("POLL_INTERVAL", "30"))
MARK_SEEN   = os.getenv("MARK_SEEN", "1") == "1"

# Allowed senders (kommasepariert)
ALLOWED_SENDERS = [s.strip().lower() for s in (os.getenv("ALLOWED_SENDERS", "")).split(",") if s.strip()]

# MinIO / S3
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT           = os.getenv("S3_ENDPOINT", "fallback")
AWS_REGION            = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET             = os.getenv("S3_BUCKET")
S3_PREFIX             = (os.getenv("S3_PREFIX") or "").lstrip("/")  # leer = flat

# Filter
ONLY_WAV       = os.getenv("ONLY_WAV", "1") == "1"
FILENAME_REGEX = (os.getenv("FILENAME_REGEX", r"^\d+_\d{8}-\d{6}\.wav$") or "").strip()

# OpenAI
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
TRANSCRIBE_MODEL = os.getenv("TRANSCRIBE_MODEL", "whisper-1")
TRANSCRIBE_LANG  = (os.getenv("TRANSCRIBE_LANG") or "").strip()

# Webhook
WEBHOOK_URL  = os.getenv("WEBHOOK_URL")
WEBHOOK_TIMEOUT = int(os.getenv("WEBHOOK_TIMEOUT", "10"))
WEBHOOK_RETRIES = int(os.getenv("WEBHOOK_RETRIES", "3"))
WEBHOOK_BACKOFF = int(os.getenv("WEBHOOK_RETRY_BACKOFF_SECONDS", "3"))

# Transcoding / MP3
CONVERT_TO_MP3      = os.getenv("CONVERT_TO_MP3", "1") == "1"   # WAV vor Upload zu MP3?
KEEP_ORIGINAL_WAV   = os.getenv("KEEP_ORIGINAL_WAV", "0") == "1" # WAV zusätzlich behalten?
MP3_BITRATE         = os.getenv("MP3_BITRATE", "96k")            # z.B. 64k, 96k, 128k
MP3_CHANNELS        = int(os.getenv("MP3_CHANNELS", "1"))        # 1=mono, 2=stereo
FFMPEG_PATH         = os.getenv("FFMPEG_PATH", "ffmpeg")         # falls nicht im PATH


# Optionale Header als "Name: Wert,Name2: Wert2"
def parse_headers(env_val: str):
    headers = {}
    for part in (env_val or "").split(","):
        if ":" in part:
            name, val = part.split(":", 1)
            headers[name.strip()] = val.strip()
    return headers

WEBHOOK_HEADERS = parse_headers(os.getenv("WEBHOOK_HEADERS", ""))

# OpenAI Client (lazy)
_openai_client = None
def openai_client() -> OpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client

# ──────────────────────────── Utility-Funktionen ────────────────────────────

def wav_bytes_to_mp3(audio_bytes: bytes, bitrate: str = "96k", channels: int = 1) -> bytes:
    """
    Konvertiert WAV-Bytes → MP3-Bytes über ffmpeg (stdin→stdout), ohne Tempfiles.
    Raises RuntimeError bei Fehlern.
    """
    cmd = [
        FFMPEG_PATH,
        "-hide_banner", "-loglevel", "error",
        "-f", "wav", "-i", "pipe:0",     # Eingang: WAV aus stdin
        "-ac", str(channels),            # Ziel-Kanalzahl
        "-b:a", bitrate,                 # Ziel-Bitrate
        "-f", "mp3", "pipe:1"            # Ausgabe: MP3 nach stdout
    ]
    try:
        proc = subprocess.run(
            cmd,
            input=audio_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        return proc.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"ffmpeg error: {e.stderr.decode('utf-8', errors='replace')}")

def connect_imap() -> IMAPClient:
    """Stellt IMAP-Verbindung her und wählt den Ziel-Ordner."""
    client = IMAPClient(IMAP_HOST, port=IMAP_PORT, ssl=True)
    client.login(IMAP_USER, IMAP_PASS)
    client.select_folder(IMAP_FOLDER)
    return client

def s3_client():
    """Erzeugt S3/MinIO-Client."""
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )

def iter_attachments(msg: Message) -> List[Tuple[str, bytes, str]]:
    """
    Sucht file-artige Parts.

    Returns:
        Liste von (filename, content_bytes, mime_type)
    """
    out = []
    for part in msg.walk():
        fn = part.get_filename()
        if not fn:
            continue
        content = part.get_payload(decode=True)
        if content is None:
            continue
        mime = part.get_content_type() or "application/octet-stream"
        out.append((fn, content, mime))
    return out

def extract_text(msg: Message) -> str:
    """Extrahiert bevorzugt 'text/plain' als String."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True) or b""
                charset = part.get_content_charset() or "utf-8"
                try:
                    return payload.decode(charset, errors="replace")
                except Exception:
                    return payload.decode(errors="replace")
    else:
        payload = msg.get_payload(decode=True) or b""
        try:
            return payload.decode(errors="replace")
        except Exception:
            return ""
    return ""

def parse_addresses(header_val: str) -> List[str]:
    """Extrahiert E-Mail-Adressen aus Header-Text (sehr einfache Heuristik)."""
    if not header_val:
        return []
    # alles innerhalb <...> oder das letzte Wort nach leerzeichen/@
    addrs = []
    for chunk in header_val.split(","):
        chunk = chunk.strip()
        m = re.search(r"<([^>]+)>", chunk)
        email_addr = (m.group(1) if m else chunk).strip().strip("\"'")
        addrs.append(email_addr.lower())
    return addrs

def is_allowed_sender(from_header: str) -> bool:
    """Prüft, ob mindestens eine Absenderadresse in ALLOWED_SENDERS ist."""
    if not ALLOWED_SENDERS:
        # Wenn keine Liste gesetzt ist, sind alle erlaubt
        return True
    senders = parse_addresses(from_header)
    return any(s in ALLOWED_SENDERS for s in senders)

def should_keep(filename: str, mime: str) -> bool:
    """Filtert nur gewünschte Anhänge (typisch: .wav & Regex-Muster)."""
    if ONLY_WAV and not filename.lower().endswith(".wav"):
        return False
    if FILENAME_REGEX:
        try:
            if not re.match(FILENAME_REGEX, filename):
                return False
        except re.error:
            # Ungültige Regex -> ignorieren
            pass
    return True

def s3_key(filename: str) -> str:
    """Flat-Key (optional mit Prefix, aber ohne Unterordnerstruktur)."""
    return f"{S3_PREFIX}{filename}" if S3_PREFIX else filename

def upload_bytes_to_s3(s3, bucket: str, key: str, data: bytes, content_type: str = None):
    """Lädt Bytes stabil via Tempfile nach S3/MinIO."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(data)
        tmp.flush()
        tmp_path = tmp.name
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    try:
        s3.upload_file(tmp_path, bucket, key, ExtraArgs=extra)
    finally:
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass

def transcribe_bytes(filename: str, audio_bytes: bytes) -> str:
    """Transkribiert Audio via OpenAI (Whisper/gpt-4o-transcribe)."""
    client = openai_client()
    bio = io.BytesIO(audio_bytes)
    bio.name = filename
    kwargs = {"model": TRANSCRIBE_MODEL, "file": bio}
    if TRANSCRIBE_LANG:
        kwargs["language"] = TRANSCRIBE_LANG
    resp = client.audio.transcriptions.create(**kwargs)
    return getattr(resp, "text", str(resp))

def post_webhook(payload: dict) -> bool:
    """
    Sendet JSON an WEBHOOK_URL mit Retries & Backoff.
    Rückgabe: True bei 2xx, sonst False.
    """
    if not WEBHOOK_URL:
        log.warning("WEBHOOK_URL nicht gesetzt; überspringe POST.")
        return False

    for attempt in range(1, WEBHOOK_RETRIES + 1):
        try:
            r = requests.post(
                WEBHOOK_URL,
                json=payload,
                headers={"Content-Type": "application/json", **WEBHOOK_HEADERS},
                timeout=WEBHOOK_TIMEOUT,
            )
            if 200 <= r.status_code < 300:
                log.info(f"→ Webhook OK (Status {r.status_code})")
                return True
            else:
                log.warning(f"Webhook Status {r.status_code}: {r.text[:300]}")

        except requests.RequestException as e:
            log.warning(f"Webhook Fehler (Versuch {attempt}/{WEBHOOK_RETRIES}): {e}")

        if attempt < WEBHOOK_RETRIES:
            time.sleep(WEBHOOK_BACKOFF)

    return False

# ──────────────────────────── Haupt-Loop ────────────────────────────

def main():
    # Sanity-Checks
    essentials = {
        "IMAP_HOST": IMAP_HOST, "IMAP_USER": IMAP_USER, "IMAP_PASS": IMAP_PASS,
        "S3_BUCKET": S3_BUCKET, "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY, "OPENAI_API_KEY": OPENAI_API_KEY,
        "WEBHOOK_URL": WEBHOOK_URL,
    }
    missing = [k for k, v in essentials.items() if not v]
    if missing:
        log.error(f"Fehlende .env Variablen: {', '.join(missing)}")
        return

    s3 = s3_client()
    parsed = urlparse(S3_ENDPOINT)
    log.info(f"S3/MinIO-Endpoint: {parsed.scheme}://{parsed.netloc} | Bucket: {S3_BUCKET}")
    if ALLOWED_SENDERS:
        log.info(f"Allowed senders: {', '.join(ALLOWED_SENDERS)}")
    else:
        log.info("Allowed senders: (nicht gesetzt) → alle Absender erlaubt")

    while True:
        try:
            with connect_imap() as server:
                log.info(f"IMAP verbunden → {IMAP_FOLDER} | Polling {INTERVAL}s")
                while True:
                    uids = server.search(["UNSEEN"])
                    if uids:
                        fetched = server.fetch(uids, ["RFC822"])
                        for uid in uids:
                            raw = fetched[uid][b"RFC822"]
                            msg = email.message_from_bytes(raw)
                            subject = msg.get("Subject", "") or ""
                            from_ = msg.get("From", "") or ""
                            date_hdr = msg.get("Date", "") or ""
                            recv_iso = datetime.now(timezone.utc).isoformat()

                            log.info(f"✉️  UID {uid} | From: {from_} | Subject: {subject}")

                            # ── Absender-Filter
                            if not is_allowed_sender(from_):
                                log.info("    → Absender nicht erlaubt. Überspringe.")
                                if MARK_SEEN:
                                    server.add_flags([uid], [b"\\Seen"])
                                continue

                            # ── Text extrahieren
                            text = extract_text(msg)
                            snippet = " ".join(text.split())[:200] + ("…" if len(text) > 200 else "")
                            log.info(f"    Text-Snippet: {snippet}")

                            # ── Attachments verarbeiten (nur passende WAVs)
                            attachments = iter_attachments(msg)
                            if not attachments:
                                log.info("    (keine Anhänge)")
                                if MARK_SEEN:
                                    server.add_flags([uid], [b"\\Seen"])
                                continue

                            uploaded_any = False

                            for filename, content, mime in attachments:
                                if not should_keep(filename, mime):
                                    log.info(f"    skip: {filename}")
                                    continue

                                base_name, ext = os.path.splitext(filename)
                                uploaded_any = False
                                key_audio = None
                                uploaded_format = None

                                # Optional: WAV -> MP3
                                audio_for_asr = content  # Default: Original für ASR
                                try:
                                    if CONVERT_TO_MP3:
                                        mp3_bytes = wav_bytes_to_mp3(
                                            content, bitrate=MP3_BITRATE, channels=MP3_CHANNELS
                                        )
                                        mp3_name = f"{base_name}.mp3"
                                        key_mp3 = s3_key(mp3_name)
                                        upload_bytes_to_s3(s3, S3_BUCKET, key_mp3, mp3_bytes, "audio/mpeg")
                                        log.info(f"    ↑ MP3 → s3://{S3_BUCKET}/{key_mp3}")
                                        uploaded_any = True
                                        key_audio = key_mp3
                                        uploaded_format = "mp3"
                                        # Für Transkription lieber MP3 verwenden (spart Bandbreite bei Remote-ASR)
                                        audio_for_asr = mp3_bytes

                                        if KEEP_ORIGINAL_WAV:
                                            key_wav = s3_key(filename)
                                            upload_bytes_to_s3(s3, S3_BUCKET, key_wav, content, mime)
                                            log.info(f"    ↑ WAV → s3://{S3_BUCKET}/{key_wav}")

                                    else:
                                        # Kein Transcoding: WAV hochladen
                                        key_wav = s3_key(filename)
                                        upload_bytes_to_s3(s3, S3_BUCKET, key_wav, content, mime)
                                        log.info(f"    ↑ WAV → s3://{S3_BUCKET}/{key_wav}")
                                        uploaded_any = True
                                        key_audio = key_wav
                                        uploaded_format = "wav"

                                except (ClientError, BotoCoreError, RuntimeError) as e:
                                    log.error(f"    [UPLOAD/TRANSCODE ERROR] {filename}: {e}")
                                    continue  # nächsten Anhang versuchen

                                # 2) Transkribieren (mit dem finalen Audioformat)
                                transcription = ""
                                try:
                                    trans_name = f"{base_name}.{uploaded_format or 'wav'}"
                                    transcription = transcribe_bytes(trans_name, audio_for_asr)
                                    log.info(f"    📝 Transkript OK ({len(transcription)} chars)")
                                except Exception as e:
                                    log.error(f"    [ASR ERROR] {filename}: {e}")

                                # 3) Webhook POST
                                payload = {
                                    "subject": subject,
                                    "text": text,
                                    "from": from_,
                                    "minio_key": key_audio,
                                    "uploaded_format": uploaded_format,
                                    "original_filename": filename,
                                    "transcription": transcription,
                                    "message_uid": int(uid),
                                    "received_at": recv_iso,
                                }
                                ok = post_webhook(payload)
                                if not ok:
                                    log.error("    Webhook fehlgeschlagen.")


                            if MARK_SEEN:
                                server.add_flags([uid], [b"\\Seen"])

                    time.sleep(INTERVAL)

        except KeyboardInterrupt:
            log.info("Beendet durch Benutzer.")
            break
        except Exception as e:
            log.warning(f"Allg. Fehler: {e} → Retry in {INTERVAL}s")
            time.sleep(INTERVAL)

# ──────────────────────────── Entry Point ────────────────────────────

if __name__ == "__main__":
    main()
