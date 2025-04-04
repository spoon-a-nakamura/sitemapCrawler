#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import csv
import hashlib
import requests
import re
import io
from bs4 import BeautifulSoup
from urllib.parse import (
    urlparse, urljoin, urlunparse
)
from collections import deque

import PyPDF2  # PDFメタ情報取得用 (pip install PyPDF2)

# ============================================
# 設定
# ============================================
START_URLS = [
    # クロール開始URL (例)
    "https://global.honda/jp/philanthropy/ideacontest/"
]

OLD_SITEMAP_FILE = "old_sitemap.csv"
NEW_SITEMAP_FILE = "new_sitemap.csv"
CACHE_DIR = "cache"

TARGET_DOMAIN = "global.honda"
TARGET_PATH_PREFIX = "/jp/philanthropy/ideacontest/"

# PDF も拾うので .pdf は含めず
EXCLUDE_EXTENSIONS = (
    ".png", ".jpg", ".jpeg", ".gif",
    ".zip", ".doc", ".docx", ".xls", ".xlsx",
    ".js", ".css"
)

MAX_PAGES = 3000
REQUESTS_TIMEOUT = 5
INTERMEDIATE_SAVE_EVERY = 50

# ============================================
# 1) URL 正規化関数
# ============================================
def canonicalize_url(url):
    """
    クエリ(?...) / フラグメント(#...) を削除し、
    /index.html or /index.htm を取り除いて末尾を '/' に統一する。

    例:
      https://.../award/2023/index.html?idea#foo -> https://.../award/2023/
      https://.../award/2021 -> https://.../award/2021/
    """
    p = urlparse(url)

    path = p.path
    # /index.html or /index.htm の削除
    if path.endswith("/index.html"):
        path = path[:-10]  # "/index.html" は10文字
    elif path.endswith("/index.htm"):
        path = path[:-9]   # "/index.htm" は9文字

    # 末尾にスラッシュが無ければ付与
    if not path.endswith("/"):
        path += "/"

    # クエリやフラグメントは捨てる
    new_url = urlunparse((
        p.scheme,
        p.netloc,
        path,
        "",  # params
        "",  # query
        ""   # fragment
    ))
    return new_url

# ============================================
# 2) ユーティリティ
# ============================================
def url_to_filename(url):
    """
    URL -> md5ハッシュ文字列に変換し、cacheファイル名を作成
    """
    md5hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    return os.path.join(CACHE_DIR, f"{md5hash}.html")

def load_old_sitemap(csv_file):
    """
    既存のサイトマップCSVを読み込み、URL列をsetで返す
    CSV列は [URL, Title, Description, Depth, Type] 想定
    """
    known = set()
    if not os.path.isfile(csv_file):
        return known

    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row and row[0]:
                known.add(row[0].strip())
    return known

def save_new_sitemap(new_entries, csv_file, mode="w"):
    """
    new_entries: [(url, title, description, depth, type_str), ...]
    CSV列 => [URL, Title, Description, Depth, Type]
    """
    if not new_entries:
        return

    write_header = False
    if not os.path.isfile(csv_file) or mode == "w":
        write_header = True

    with open(csv_file, mode, newline="", encoding="utf-8-sig") as fp:
        writer = csv.writer(fp)
        if write_header:
            writer.writerow(["URL", "Title", "Description", "Depth", "Type"])
        for (url, title, desc, depth, t_str) in new_entries:
            writer.writerow([url, title, desc, depth, t_str])

def fetch_html(url, use_cache=True):
    """
    HTMLページを requests で取得し、text/htmlならcacheに保存。
    """
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    cache_file = url_to_filename(url)
    if use_cache and os.path.isfile(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return f.read()

    html = None
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                "AppleWebKit/537.36 (KHTML, like Gecko)"
                "Chrome/91.0.4472.124 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()

        ctype = resp.headers.get("Content-Type", "").lower()
        charset_match = re.search(r"charset\s*=\s*([^\s;]+)", ctype)
        if charset_match:
            resp.encoding = charset_match.group(1)
        else:
            resp.encoding = resp.apparent_encoding

        if "text/html" in ctype:
            html = resp.text
    except Exception as e:
        print(f"[requests error] {url} => {e}")
        html = None

    if html:
        with open(cache_file, "w", encoding="utf-8") as f:
            f.write(html)

    return html

def extract_title_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("title")
    if title_tag and title_tag.get_text(strip=True):
        return title_tag.get_text(strip=True)
    return "No Title"

def extract_description_from_html(html):
    """
    meta name="description" or property="og:description" などを探す
    """
    soup = BeautifulSoup(html, "html.parser")

    meta_name_desc = soup.find("meta", attrs={"name": re.compile(r"^description$", re.IGNORECASE)})
    if meta_name_desc and meta_name_desc.get("content"):
        return meta_name_desc["content"].strip()

    meta_og_desc = soup.find("meta", attrs={"property": re.compile(r"^og:description$", re.IGNORECASE)})
    if meta_og_desc and meta_og_desc.get("content"):
        return meta_og_desc["content"].strip()

    return "No Description"

def extract_links(current_url, html):
    """
    同一ドメイン & /jp/philanthropy/ideacontest/ 以下
    かつ EXCLUDE_EXTENSIONS に無いリンクを抽出。
    PDFも含む。
    """
    soup = BeautifulSoup(html, "html.parser")
    found = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        # 絶対URLに
        abs_url = urljoin(current_url, href)

        # 正規化 (index.htmlや?queryを削除)
        abs_url = canonicalize_url(abs_url)

        parsed = urlparse(abs_url)
        if parsed.netloc != TARGET_DOMAIN:
            continue
        if not parsed.path.startswith(TARGET_PATH_PREFIX):
            continue

        path_lower = parsed.path.lower()
        if any(path_lower.endswith(ext) for ext in EXCLUDE_EXTENSIONS):
            continue

        found.add(abs_url)

    return found

def get_pdf_title(pdf_url):
    """
    PDF をダウンロードしてメタ情報(/Title)を取得
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                "AppleWebKit/537.36 (KHTML, like Gecko)"
                "Chrome/91.0.4472.124 Safari/537.36"
            )
        }
        resp = requests.get(pdf_url, headers=headers, timeout=REQUESTS_TIMEOUT, stream=True)
        resp.raise_for_status()

        with io.BytesIO(resp.content) as f:
            pdf_reader = PyPDF2.PdfReader(f)
            metadata = pdf_reader.metadata
            if metadata and "/Title" in metadata:
                return metadata["/Title"].strip()
    except Exception as e:
        print(f"[PDF Title Error] {pdf_url} => {e}")
    return "No PDF Title"

# ============================================
# BFS
# ============================================
def crawl(known_urls, start_urls):
    """
    BFSしつつ、PDFリンクなら depth+0.5 でget_pdf_title。
    CSV => [ (url, title, desc, depth, type_str), ... ]
    """
    visited = set()
    new_discovered = []
    queue = deque()

    # 開始URLを depth=1 でキューに (正規化して)
    for url in start_urls:
        canon = canonicalize_url(url)
        queue.append((canon, 1.0))

    while queue and len(visited) < MAX_PAGES:
        current_url, depth = queue.popleft()

        # 重複チェック
        if current_url in visited or current_url in known_urls:
            continue

        visited.add(current_url)
        print(f"Crawling (depth={depth}): {current_url}")

        parsed = urlparse(current_url)
        path_lower = parsed.path.lower()

        # -------- PDF の場合 --------
        if path_lower.endswith(".pdf"):
            pdf_title = get_pdf_title(current_url)
            pdf_desc = "No Description (PDF)"
            new_discovered.append((current_url, pdf_title, pdf_desc, depth, "PDF"))
            known_urls.add(current_url)
            # BFSしない
            continue

        # -------- HTML の場合 --------
        page_html = fetch_html(current_url, use_cache=True)
        if not page_html:
            continue

        title = extract_title_from_html(page_html)
        desc = extract_description_from_html(page_html)
        new_discovered.append((current_url, title, desc, depth, "Page"))
        known_urls.add(current_url)

        # 子リンクを抽出 (正規化済み)
        child_links = extract_links(current_url, page_html)
        for link in child_links:
            if link not in visited and link not in known_urls:
                if link.lower().endswith(".pdf"):
                    # PDF => depth+0.5
                    visited.add(link)
                    known_urls.add(link)
                    pdf_title2 = get_pdf_title(link)
                    pdf_desc2 = "No Description (PDF)"
                    new_discovered.append((link, pdf_title2, pdf_desc2, depth+0.5, "PDF"))
                else:
                    queue.append((link, depth + 1))

        # 途中保存
        if len(visited) % INTERMEDIATE_SAVE_EVERY == 0:
            print("[Intermediate Save]")
            save_new_sitemap(new_discovered, NEW_SITEMAP_FILE, mode="w")

    return new_discovered

def main():
    known_urls = load_old_sitemap(OLD_SITEMAP_FILE)
    print(f"Starting BFS from {START_URLS} ...")

    new_pages = crawl(known_urls, START_URLS)

    print(f"\n[Done BFS] Found {len(new_pages)} new page(s).")
    if new_pages:
        print(f"Saving new pages to {NEW_SITEMAP_FILE} ...")
        save_new_sitemap(new_pages, NEW_SITEMAP_FILE, mode="w")

    print("Finished.")

if __name__ == "__main__":
    main()
