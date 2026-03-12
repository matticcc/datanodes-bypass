import asyncio
import os
import re
from urllib.parse import urlparse, unquote
from playwright.async_api import async_playwright
from tqdm import tqdm

INPUT_FILE = "links.txt"
OUTPUT_FILE = "output.txt"
MAX_WORKERS = 3

def is_valid_download(url: str) -> bool:
    parsed = urlparse(url)
    return bool(parsed.scheme in ["http", "https"] and parsed.netloc)

async def wait_for_download_response(context, page):
    try:
        response = await context.wait_for_event(
            "response",
            lambda resp: (
                "https://datanodes.to/download" in resp.url and
                resp.request.method == "POST" and
                "application/json" in resp.headers.get("content-type", "")
            ),
            timeout=30000
        )
        data = await response.json()
        raw_url = data.get("url") or data.get("downloadUrl") or data.get("link")
        return unquote(raw_url) if raw_url else None
    except Exception:
        return None

async def process_link(context, link, main_page):
    try:
        try:
            await main_page.goto(link, wait_until="domcontentloaded", timeout=30000)
        except Exception as e:
            if "ERR_ABORTED" not in str(e):
                raise e

        if await main_page.locator('text=Bad Gateway').is_visible() or await main_page.locator('text=Error 502').is_visible():
            await main_page.reload(wait_until="domcontentloaded")

        continue_button = main_page.locator("#method_free")
        await continue_button.wait_for(state="visible", timeout=20000)
        
        await main_page.wait_for_timeout(4000)
        await continue_button.evaluate("el => el.click()")
        await main_page.wait_for_timeout(2000)
        
        current_page = main_page
        for p in context.pages:
            if "datanodes" in p.url and p != main_page:
                current_page = p
                await current_page.bring_to_front()
                break

        await current_page.wait_for_load_state("domcontentloaded")

        download_button = current_page.locator("a, button").filter(has_text=re.compile(r"Download", re.IGNORECASE)).filter(has_not_text=re.compile(r"Premium", re.IGNORECASE)).first
        await download_button.wait_for(state="visible", timeout=15000)
        
        for _ in range(3):
            await download_button.click(force=True)
            await current_page.wait_for_timeout(1000)
            for p in context.pages:
                if p != current_page and p != main_page:
                    try:
                        await p.close()
                    except:
                        pass

        await current_page.wait_for_timeout(6000)

        wait_task = asyncio.create_task(wait_for_download_response(context, current_page))

        final_continue_button = current_page.locator("a, button").filter(has_text=re.compile(r"continue", re.IGNORECASE)).first
        await final_continue_button.wait_for(state="visible", timeout=15000)
        
        await final_continue_button.evaluate("el => el.click()")
        await current_page.wait_for_timeout(1000)
        
        for p in context.pages:
            if p != current_page and p != main_page:
                try:
                    await p.close()
                except:
                    pass

        download_url = await wait_task
        if download_url:
            download_url = download_url.replace("%0A", "").replace("\n", "")

        if download_url and is_valid_download(download_url):
            return download_url
            
    except Exception:
        pass 

    return None

async def worker(browser, queue, extracted, pbar, stats):
    context = await browser.new_context(accept_downloads=False)
    
    async def handle_pages(new_page):
        await asyncio.sleep(1.5)
        if not new_page.is_closed() and "datanodes" not in new_page.url:
            try:
                await new_page.close()
            except:
                pass
                
    context.on("page", lambda p: asyncio.create_task(handle_pages(p)))
    
    main_page = await context.new_page()
    
    while True:
        try:
            idx, link = await queue.get()
            try:
                result = await process_link(context, link, main_page)
                if result:
                    extracted[idx] = result
                    stats["success"] += 1
                else:
                    extracted[idx] = f"# Failed to extract from {link}"
            except Exception:
                extracted[idx] = f"# Error processing {link}"
            finally:
                pbar.set_postfix(Success=stats["success"])
                pbar.update(1)
                queue.task_done()
        except asyncio.CancelledError:
            await context.close()
            break

async def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File {INPUT_FILE} non trovato!")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        links = [line.strip() for line in f if line.strip()]

    if not links:
        print("❌ Nessun link in links.txt!")
        return

    extracted = [None] * len(links)
    stats = {"success": 0}
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        queue = asyncio.Queue()
        for idx, link in enumerate(links):
            await queue.put((idx, link))

        print("Playwright opened in background")
        with tqdm(total=len(links), desc="Progress", unit="link", dynamic_ncols=True) as pbar:
            pbar.set_postfix(Success=0)
            active_workers = min(MAX_WORKERS, len(links))
            workers = [asyncio.create_task(worker(browser, queue, extracted, pbar, stats)) for _ in range(active_workers)]
            
            await queue.join()
            for worker_task in workers:
                worker_task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        await browser.close()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(extracted))

    print(f"Done. {stats['success']} links extracted out of {len(links)}.")
    print(f"Results saved in: {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
