from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import urllib.parse
import requests
import binascii
import os
import re
from urllib.parse import urljoin
from anyio import to_thread
from requests.exceptions import ConnectionError, RequestException
from urllib3.exceptions import IncompleteRead
import time
import logging
from contextlib import contextmanager
from multiprocessing import Manager
import sqlite3
from datetime import datetime, timedelta
import asyncio
from cachetools import TTLCache

# Configuração de logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Limite de uma conexão simultânea entre todos os workers
manager = Manager()
connection_limit = manager.Semaphore(1)  # Apenas 1 conexão por vez

# Caminho do SQLite no Hugging Face Spaces (usar /tmp para escrita garantida)
DB_PATH = "/tmp/cache.db"

# Cache em memória como fallback
memory_cache = TTLCache(maxsize=100, ttl=300)

def init_db():
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)  # Criar diretório, se necessário
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.execute("PRAGMA journal_mode=WAL")  # Habilitar Write-Ahead Logging para concorrência
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    cache_key TEXT PRIMARY KEY,
                    data BLOB,
                    expires_at TIMESTAMP
                )
            """)
            conn.commit()
            logging.debug("[HLS Proxy] Banco SQLite inicializado com sucesso")
    except sqlite3.OperationalError as e:
        logging.error(f"[HLS Proxy] Erro ao inicializar SQLite: {e}")
        # Não falhar na inicialização; usar cache em memória como fallback

# Inicializar o banco na inicialização
@app.on_event("startup")
async def startup():
    init_db()
    # Iniciar tarefa de limpeza periódica
    asyncio.create_task(cleanup_cache())

# Tarefa periódica para limpar cache expirado
async def cleanup_cache():
    while True:
        try:
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                conn.execute("DELETE FROM cache WHERE expires_at < ?", (datetime.now(),))
                conn.commit()
                logging.debug("[HLS Proxy] Cache expirado limpo do SQLite")
        except Exception as e:
            logging.error(f"[HLS Proxy] Erro ao limpar cache: {e}")
        await asyncio.sleep(300)  # Limpar a cada 5 minutos

DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"

@contextmanager
def session_manager():
    session = requests.Session()
    try:
        yield session
    finally:
        try:
            session.close()
            logging.debug("Sessão HTTP fechada")
        except:
            pass

def get_ip(request):
    forwarded_for = request.headers.get("x-forwarded-for")
    real_ip = request.headers.get("x-real-ip")
    if forwarded_for:
        ip = forwarded_for.split(",")[0].strip()
    elif real_ip:
        ip = real_ip
    else:
        ip = request.client.host
    return ip

def get_cache_key(client_ip: str, url: str) -> str:
    return f"{client_ip}:{url}"

def rewrite_m3u8_urls(playlist_content: str, base_url: str, request: Request) -> str:
    def replace_url(match):
        segment_url = match.group(0).strip()
        if segment_url.startswith('#') or not segment_url or segment_url == '/':
            return segment_url
        try:
            absolute_url = urljoin(base_url + '/', segment_url)
            if not (absolute_url.endswith('.ts') or '/hl' in absolute_url.lower() or absolute_url.endswith('.m3u8')):
                logging.debug(f"[HLS Proxy] Ignorando URL inválida no m3u8: {absolute_url}")
                return segment_url
            scheme = request.url.scheme
            host = request.url.hostname
            port = request.url.port or (443 if scheme == 'https' else 80)
            proxied_url = f"{scheme}://{host}:{port}/proxy?url={urllib.parse.quote(absolute_url)}"
            return proxied_url
        except ValueError as e:
            logging.debug(f"[HLS Proxy] Erro ao resolver URL {segment_url}: {e}")
            return segment_url
    return re.sub(r'^(?!#)\S+', replace_url, playlist_content, flags=re.MULTILINE)

async def stream_response(response, url: str, headers: dict, sess: requests.Session):
    def generate_chunks(response):
        try:
            for chunk in response.iter_content(chunk_size=4095):
                if chunk:
                    yield chunk
        except (IncompleteRead, ConnectionError) as e:
            logging.debug(f"[HLS Proxy] Erro ao processar chunks: {e}")
        except Exception as e:
            logging.debug(f"[HLS Proxy] Erro inesperado ao processar chunks: {e}")
        finally:
            try:
                sess.close()
                logging.debug("Sessão fechada em stream_response")
            except:
                pass

    iterator = generate_chunks(response)
    while True:
        try:
            chunk = await to_thread.run_sync(lambda: next(iterator, None))
            if chunk is None:
                break
            yield chunk
        except StopIteration:
            break

@app.get("/proxy")
async def proxy(url: str, request: Request):
    client_ip = get_ip(request)
    cache_key = get_cache_key(client_ip, url) if '.mp4' in url.lower() or '.m3u8' in url.lower() else client_ip
    if not url:
        raise HTTPException(status_code=400, detail="No URL provided")

    # Verificar cache em memória (fallback)
    if cache_key in memory_cache:
        logging.debug(f"[HLS Proxy] Retornando do cache em memória para {cache_key}")
        media_type = 'application/x-mpegURL' if '.m3u8' in url.lower() else 'video/mp4'
        return StreamingResponse(
            content=iter([memory_cache[cache_key]]),
            media_type=media_type
        )

    # Verificar cache no SQLite
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            cursor = conn.execute("SELECT data FROM cache WHERE cache_key = ? AND expires_at > ?", (cache_key, datetime.now()))
            cached_response = cursor.fetchone()
            if cached_response:
                logging.debug(f"[HLS Proxy] Retornando do cache SQLite para {cache_key}")
                media_type = 'application/x-mpegURL' if '.m3u8' in url.lower() else 'video/mp4'
                return StreamingResponse(
                    content=iter([cached_response[0]]),
                    media_type=media_type
                )
    except sqlite3.OperationalError as e:
        logging.error(f"[HLS Proxy] Erro ao acessar SQLite: {e}")

    default_headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept-Encoding": "identity",
        "Accept": "*/*",
        "Connection": "close"  # Evita conexões persistentes
    }

    with connection_limit:  # Limita a 1 conexão simultânea
        with session_manager() as session:
            session.headers.update(default_headers)
            max_retries = 2
            attempts = 0
            tried_without_range = False

            while attempts < max_retries:
                if not ('.m3u8' in url.lower() or '.mp4' in url.lower() or '.ts' in url.lower() or '/hl' in url.lower()):
                    logging.debug(f"[HLS Proxy] URL inválida: {url}")
                    raise HTTPException(status_code=400, detail="Nenhuma URL compatível com o proxy")

                try:
                    range_header = request.headers.get('Range')
                    if '.mp4' in url.lower() and range_header and not tried_without_range:
                        default_headers['Range'] = range_header
                    else:
                        default_headers.pop('Range', None)

                    logging.debug(f"[HLS Proxy] Iniciando requisição para {url} por {client_ip}")
                    response = session.get(url, headers=default_headers, allow_redirects=True, station=True, timeout=10)

                    if response.status_code in (200, 206):
                        content_type = response.headers.get('content-type', '').lower()

                        if 'application/x-mpegURL' in content_type or 'application/vnd.apple.mpegurl' in content_type or '.m3u8' in url.lower():
                            base_url = url.rsplit('/', 1)[0]
                            playlist_content = response.content.decode('utf-8', errors='ignore')
                            rewritten_playlist = rewrite_m3u8_urls(playlist_content, base_url, request)
                            # Salvar no cache SQLite
                            try:
                                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                                    expires_at = datetime.now() + timedelta(seconds=300)
                                    conn.execute(
                                        "INSERT OR REPLACE INTO cache (cache_key, data, expires_at) VALUES (?, ?, ?)",
                                        (cache_key, rewritten_playlist.encode('utf-8'), expires_at)
                                    )
                                    conn.commit()
                            except sqlite3.OperationalError as e:
                                logging.error(f"[HLS Proxy536] Erro ao salvar no SQLite: {e}")
                                memory_cache[cache_key] = rewritten_playlist.encode('utf-8')  # Fallback para cache em memória
                            return StreamingResponse(
                                content=iter([rewritten_playlist.encode('utf-8')]),
                                media_type='application/x-mpegURL'
                            )

                        response_headers = {
                            key: value for key, value in response.headers.items()
                            if key.lower() in ('content-type', 'accept-ranges', 'content-range')
                        }
                        response_headers['Connection'] = 'close'

                        media_type = (
                            'video/mp4' if '.mp4' in url.lower()
                            else 'video/mp2t' if '.ts' in url.lower() or '/hl' in url
                            else response_headers.get('content-type', 'application/octet-stream')
                        )
                        status_code = 206 if response.status_code == 206 else 200

                        if response.status_code == 206 and 'Content-Range' in response.headers:
                            response_headers['Content-Range'] = response.headers.get('Content-Range', '')

                        return StreamingResponse(
                            content=stream_response(response, url, default_headers, session),
                            media_type=media_type,
                            headers=response_headers,
                            status_code=status_code
                        )

                    elif response.status_code == 416:
                        if range_header and not tried_without_range:
                            tried_without_range = True
                            continue
                        else:
                            raise HTTPException(status_code=416, detail="Range Not Satisfiable")

                    elif response.status_code == 404 and ('.ts' in url.lower() or '/hl' in url.lower()):
                        time.sleep(1)
                    else:
                        time.sleep(1)
                    attempts += 1
                except RequestException as e:
                    logging.error(f"[HLS Proxy] Erro ao processar {url}: {e}")
                    time.sleep(1)
                    attempts += 1

            raise HTTPException(status_code=502, detail="Falha ao conectar após múltiplas tentativas")

@app.head("/proxy")
async def proxy_head(url: str, request: Request):
    return Response(status_code=200)

@app.get("/check")
async def check(url: str, request: Request):
    if '.m3u8' in url or 'get.php' in url:
        default_headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Encoding": "identity",
            "Accept": "*/*",
            "Connection": "close"
        }
        with session_manager() as session:
            session.headers.update(default_headers)
            try:
                response = session.get(url, headers=default_headers, allow_redirects=True, stream=True, timeout=10)
                if response.status_code != 200:
                    default_headers.update({'User-Agent': binascii.b2a_hex(os.urandom(20))[:32]})
                    response = session.get(url, headers=default_headers, allow_redirects=True, stream=True, timeout=10)
                return {'code': response.status_code}
            except Exception as e:
                logging.error(f"[HLS Proxy] Erro no check {url}: {e}")
                return {'code': 'error'}
    else:
        return {'message': 'only m3u8 links'}

@app.get("/")
def main_index():
    return {"message": "PROXY ONEPLAY VIP ver: 1.0.5"}
