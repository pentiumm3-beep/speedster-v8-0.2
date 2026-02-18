
import os
import sys
import asyncio
import aiohttp
import uvloop
import argparse
import re
import time
import gc
from tqdm.asyncio import tqdm

# Mengaktifkan Ultra-Fast Event Loop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class SpeedsterMasterpiece:
    def __init__(self, url, output=None, connections=None, token=None):
        self.url = url
        self.output = output
        self.token = token
        self.connections = connections or (24 if 'google.colab' in sys.modules else 8)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Encoding": "identity",
            "Referer": "https://civitai.com/"
        }
        if self.token: self.headers["Authorization"] = f"Bearer {self.token}"

    def get_filename_from_cd(self, cd):
        if not cd: return None
        fname = re.findall('filename="?([^"]+)"?', cd)
        if len(fname) == 0: fname = re.findall('filename\*=UTF-8\'\'(.+)', cd)
        return fname[0] if fname else None

    def release_disk_cache(self, filepath):
        """
        FITUR BARU: Memberitahu Kernel Linux untuk membuang file dari RAM (Page Cache).
        Ini mencegah OOM saat meload model besar setelah download.
        """
        try:
            with open(filepath, 'rb') as f:
                # POSIX_FADV_DONTNEED = 4 (Memberitahu OS bahwa data tidak akan diakses dalam waktu dekat)
                os.posix_fadvise(f.fileno(), 0, os.path.getsize(filepath), os.POSIX_FADV_DONTNEED)
            # Paksa Garbage Collector Python
            gc.collect()
        except Exception:
            # Fitur ini hanya jalan di Linux (Colab), abaikan jika di Windows
            pass

    async def get_info(self, session):
        try:
            async with session.head(self.url, headers=self.headers, allow_redirects=True) as resp:
                final_url = str(resp.url)
                size = int(resp.headers.get('Content-Length', 0))
                resumable = resp.headers.get('Accept-Ranges') == 'bytes'
                detected_name = self.get_filename_from_cd(resp.headers.get('Content-Disposition'))
                if not detected_name:
                    detected_name = os.path.basename(final_url).split("?")[0] or "downloaded_file.bin"
                return size, resumable, final_url, detected_name
        except Exception as e:
            print(f"❌ Error info: {e}")
            sys.exit(1)

    async def download_chunk(self, session, url, start, end, file_obj, pbar, chunk_id):
        headers = self.headers.copy()
        headers['Range'] = f'bytes={start}-{end}'
        max_retries = 10
        for attempt in range(max_retries):
            try:
                async with session.get(url, headers=headers) as resp:
                    resp.raise_for_status()
                    file_obj.seek(start)
                    async for data in resp.content.iter_chunked(65536):
                        file_obj.write(data)
                        pbar.update(len(data))
                    return
            except Exception as e:
                wait_time = (attempt + 1) * 2
                if attempt > 2: pbar.write(f"⚠️ Chunk {chunk_id} retry {attempt+1}...")
                await asyncio.sleep(wait_time)
        raise Exception(f"Chunk {chunk_id} failed.")

    async def run(self):
        connector = aiohttp.TCPConnector(limit=self.connections, force_close=False, enable_cleanup_closed=True)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=60, sock_read=60)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            size, resumable, final_url, detected_name = await self.get_info(session)
            final_output = self.output if self.output else detected_name
            
            print(f"🚀 SPEEDSTER V8.1 | Target: {final_output}")
            print(f"💾 Size: {size/1024/1024:.2f} MB | Optimization: Anti-OOM Mode")

            with open(final_output, "wb") as f: f.truncate(size)
            
            pbar = tqdm(total=size, unit='B', unit_scale=True, unit_divisor=1024, desc="⚡ DOWNLOADING", dynamic_ncols=True)
            
            with open(final_output, "rb+") as f:
                if resumable and size > 0:
                    chunk_size = size // self.connections
                    tasks = [self.download_chunk(session, final_url, i*chunk_size, (i*chunk_size)+chunk_size-1 if i!=self.connections-1 else size-1, f, pbar, i) for i in range(self.connections)]
                    await asyncio.gather(*tasks)
                else:
                    await self.download_chunk(session, final_url, 0, size-1, f, pbar, 0)
            
            pbar.close()
            
            # --- STEP PENTING: FLUSH RAM ---
            print("🧹 Membersihkan RAM Cache (Agar tidak OOM)...")
            self.release_disk_cache(os.path.abspath(final_output))
            print(f"✅ Selesai! File aman digunakan.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url"); parser.add_argument("-o", "--output"); parser.add_argument("-c", "--conns", type=int); parser.add_argument("-t", "--token")
    args = parser.parse_args()
    try: asyncio.run(SpeedsterMasterpiece(args.url, args.output, args.conns, args.token).run())
    except KeyboardInterrupt: pass
    except Exception as e: print(f"\n❌ Error: {e}")

if __name__ == "__main__": main()
