import os, asyncio, time, re, io, json
from pathlib import Path
from typing import List, Dict
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import FSInputFile, InputMediaPhoto
from aiogram.exceptions import TelegramRetryAfter
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from tenacity import retry, wait_exponential, stop_after_attempt
from tqdm import tqdm
import fitz  # PyMuPDF

# ================== الإعداد ==================
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SESSION_NAME = os.getenv("SESSION_NAME", "tg_pdf_img_session")
WORK_DIR = Path(os.getenv("WORK_DIR", "workdir"))
DEFAULT_CHANNEL = os.getenv("CHANNEL", "").strip()
DPI = int(os.getenv("DPI", "150"))
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "85"))
ALBUM_BATCH_SIZE = int(os.getenv("ALBUM_BATCH_SIZE", "10"))  # حد تيليجرام للألبوم = 10
PAUSE_BETWEEN_ALBUMS = float(os.getenv("PAUSE_BETWEEN_ALBUMS", "1"))
PAUSE_BETWEEN_PAGES = float(os.getenv("PAUSE_BETWEEN_PAGES", "0"))
SEPARATOR_TEXT = os.getenv("SEPARATOR_TEXT", "────────  فاصل بين الملفات  ────────")
# ============================================

SAFE_CHARS = r"[^a-zA-Z0-9\u0600-\u06FF\-\_\.\s]"

def safe_filename(name: str) -> str:
    name = re.sub(SAFE_CHARS, "_", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name[:150]

def build_pdf_filename(msg) -> str:
    base = None
    if msg.document:
        for attr in (msg.document.attributes or []):
            if getattr(attr, "file_name", None):
                base = attr.file_name
                break
    if not base:
        cap = (msg.message or "").strip()
        base = cap if cap else f"file_{msg.id}.pdf"
    if not base.lower().endswith(".pdf"):
        base += ".pdf"
    return safe_filename(base)

async def collect_pdf_ids(client: TelegramClient, channel: str) -> List[int]:
    ids = []
    async for msg in client.iter_messages(channel, limit=None):
        if msg.document and msg.document.mime_type == "application/pdf":
            ids.append(msg.id)
    ids.sort()
    return ids

@retry(wait=wait_exponential(multiplier=1, min=3, max=60), stop=stop_after_attempt(5))
async def dl_with_retry(client: TelegramClient, msg, target: Path):
    await client.download_media(msg, file=str(target))

def pdf_to_images(pdf_path: Path, out_dir: Path, dpi: int = DPI, quality: int = JPEG_QUALITY) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    images = []
    with fitz.open(pdf_path) as doc:
        zoom = dpi / 72.0
        matrix = fitz.Matrix(zoom, zoom)
        for i, page in enumerate(doc):
            img_path = out_dir / f"page_{i+1:05d}.jpg"
            if img_path.exists() and img_path.stat().st_size > 0:
                images.append(img_path)
                continue
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            pix.save(str(img_path), jpg_quality=quality)
            images.append(img_path)
    return images

def load_progress(prog_file: Path) -> Dict[str, int]:
    if prog_file.exists():
        try:
            return json.loads(prog_file.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_progress(prog_file: Path, data: Dict[str, int]):
    prog_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

async def send_album(bot: Bot, chat_id: int, images: List[Path], caption: str = None):
    # Telegram يسمح بـ 10 عناصر/ألبوم
    for i in range(0, len(images), ALBUM_BATCH_SIZE):
        group = images[i:i+ALBUM_BATCH_SIZE]
        media = []
        for j, p in enumerate(group):
            media.append(
                InputMediaPhoto(
                    media=FSInputFile(str(p)),
                    caption=caption if (j == 0 and caption) else None
                )
            )
            if PAUSE_BETWEEN_PAGES > 0:
                await asyncio.sleep(PAUSE_BETWEEN_PAGES)
        # إعادة محاولات تلقائية عند حدود الإرسال
        while True:
            try:
                await bot.send_media_group(chat_id, media=media)
                break
            except TelegramRetryAfter as e:
                await asyncio.sleep(getattr(e, "retry_after", 5))
            except Exception:
                await asyncio.sleep(3)
                continue
        if PAUSE_BETWEEN_ALBUMS > 0:
            await asyncio.sleep(PAUSE_BETWEEN_ALBUMS)

async def handle_pdf_inline(bot: Bot, chat_id: int, pdf_path: Path, prog_file: Path):
    images_dir = pdf_path.parent.parent / "images" / pdf_path.stem
    images = pdf_to_images(pdf_path, images_dir, dpi=DPI, quality=JPEG_QUALITY)
    total_pages = len(images)
    caption = f"{pdf_path.name} — {total_pages} صفحة"

    progress = load_progress(prog_file)
    sent_upto = int(progress.get(pdf_path.stem, 0))  # آخر صفحة أُرسلت
    remaining = images[sent_upto:] if sent_upto < total_pages else []
    if not remaining:
        await bot.send_message(chat_id, f"تم إرسال {pdf_path.name} سابقًا ({total_pages} صفحة). أتخطّاه.")
        return

    await bot.send_message(chat_id, f"إرسال {pdf_path.name}: {total_pages} صفحة (متبقي {len(remaining)}).")

    for i in range(0, len(remaining), ALBUM_BATCH_SIZE):
        batch = remaining[i:i+ALBUM_BATCH_SIZE]
        await send_album(bot, chat_id, batch, caption=caption if (i == 0) else None)
        sent_upto += len(batch)
        progress[pdf_path.stem] = sent_upto
        save_progress(prog_file, progress)
        await bot.send_message(chat_id, f"{pdf_path.name}: تم إرسال {sent_upto}/{total_pages} صفحة.")
    # فاصل بين الملفات
    if SEPARATOR_TEXT:
        await bot.send_message(chat_id, SEPARATOR_TEXT)

async def download_and_send_inline(bot: Bot, chat_id: int, channel: str):
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    pdf_dir = WORK_DIR / "pdfs"; pdf_dir.mkdir(parents=True, exist_ok=True)
    prog_file = WORK_DIR / ".progress.json"

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()

    pdf_ids = await collect_pdf_ids(client, channel)
    if not pdf_ids:
        await bot.send_message(chat_id, "لم أجد ملفات PDF في القناة.")
        return

    await bot.send_message(chat_id, f"وجدت {len(pdf_ids)} ملف PDF. أبدأ بالتحويل والإرسال داخل المحادثة…")

    pbar = tqdm(total=len(pdf_ids), unit="pdf", desc="Processing PDFs")
    for mid in pdf_ids:
        try:
            msg = await client.get_messages(channel, ids=mid)
            if not msg or not msg.document or msg.document.mime_type != "application/pdf":
                pbar.update(1); continue
            filename = build_pdf_filename(msg)
            target = pdf_dir / filename

            if not target.exists():
                try:
                    await dl_with_retry(client, msg, target)
                except FloodWaitError as e:
                    wait_for = int(getattr(e, "seconds", 30))
                    time.sleep(wait_for)
                    await dl_with_retry(client, msg, target)

            if target.exists() and target.stat().st_size > 0:
                await handle_pdf_inline(bot, chat_id, target, prog_file)
                await asyncio.sleep(PAUSE_BETWEEN_ALBUMS)
        except Exception as ex:
            await bot.send_message(chat_id, f"[خطأ] في ملف PDF (msg_id={mid}): {ex}")
        finally:
            pbar.update(1)
    pbar.close()
    await bot.send_message(chat_id, "اكتمل إرسال كل الصور في المحادثة.")

async def main():
    if not (API_ID and API_HASH and BOT_TOKEN):
        raise RuntimeError("رجاءً اضبط API_ID, API_HASH, BOT_TOKEN في ملف .env")
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    user_channel = {}

    @dp.message(Command("start"))
    async def start_cmd(m: types.Message):
        await m.answer(
            "أهلًا! هذا البوت يرسل **كل صفحات PDF كصور داخل المحادثة** فقط.\n"
            "استخدم: /setchannel @اسم_القناة ثم /download"
        )

    @dp.message(Command("setchannel"))
    async def set_channel_cmd(m: types.Message):
        parts = m.text.split(maxsplit=1)
        if len(parts) < 2:
            return await m.answer("اكتب: /setchannel @ChannelUsername أو رابط https://t.me/...")
        ch = parts[1].strip()
        user_channel[m.from_user.id] = ch
        await m.answer(f"تم حفظ القناة: {ch}")

    @dp.message(Command("download"))
    async def download_cmd(m: types.Message):
        ch = user_channel.get(m.from_user.id) or DEFAULT_CHANNEL
        if not ch:
            return await m.answer("لم يتم تحديد قناة. استخدم /setchannel @ChannelUsername أولًا.")
        await m.answer(f"تم تحديد القناة: {ch}\nأبدأ بجمع الـPDF وتحويلها وإرسالها كصور…")
        try:
            await download_and_send_inline(bot, m.chat.id, ch)
        except Exception as ex:
            await m.answer(f"حدث خطأ أثناء العملية: {ex}")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
