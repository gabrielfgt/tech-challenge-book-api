# scripts/scraper_lib.py
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
import csv
import re
import hashlib
import mimetypes
import time
import os
import logging
import base64
from typing import List, Dict, Tuple, Optional

logger = logging.getLogger("book_scraper")
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

BASE_URL = "https://books.toscrape.com/"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BookScraper/1.0; +https://example.com/bot)"
}

RATING_MAP = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5
}

def safe_slug(text: Optional[str], maxlen: int = 50) -> str:
    if not text:
        return "unknown"
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text[:maxlen].strip("-")


def parse_price(text: str) -> Optional[float]:
    txt = text.replace("£", "").replace("Â", "").strip()
    try:
        return float(txt)
    except Exception:
        return None


def extract_rating_from_tag(book_soup: BeautifulSoup) -> Optional[int]:
    p = book_soup.find("p", class_="star-rating")
    if p:
        classes = p.get("class", [])
        for cls in classes:
            if cls in RATING_MAP:
                return RATING_MAP[cls]
    for name, val in RATING_MAP.items():
        if book_soup.find("p", class_=f"star-rating {name}"):
            return val
    return None

def get_extension_from_url_or_ct(url: str, resp: requests.Response) -> str:
    path = urlparse(url).path
    ext = Path(path).suffix
    if ext:
        return ext.lower()
    ct = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ct:
        ext = mimetypes.guess_extension(ct)
        if ext:
            return ext
    return ".jpg"

def create_session(headers: Optional[Dict[str, str]] = None) -> requests.Session:
    s = requests.Session()
    s.headers.update(headers or DEFAULT_HEADERS)
    return s

def load_page(session: requests.Session, url: str, timeout: int = 20) -> BeautifulSoup:
    resp = session.get(url, timeout=timeout)
    if resp.encoding is None or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = resp.apparent_encoding or "utf-8"
    return BeautifulSoup(resp.text, "html.parser")

def get_categories(session: requests.Session, base_url: str = BASE_URL) -> List[Tuple[str, str]]:
    parser = load_page(session, base_url)
    links = parser.select("ul.nav.nav-list ul li a")
    cats = []
    for a in links:
        href = a.get("href")
        name = a.get_text(strip=True)
        if href:
            cats.append((name, href))
    logger.info("Found %d categories", len(cats))
    return cats


def get_books(session: requests.Session, category_href_or_url: str, base_url: str = BASE_URL,
              per_page_delay: float = 0.3) -> List[Dict]:
    """
    Itera todas as páginas de uma categoria (segue li.next a) e retorna lista de livros.
    Cada livro: dict { title, price, rating, category, image } where image will initially be the image URL.
    """
    books_data: List[Dict] = []
    page_url = urljoin(base_url, category_href_or_url)

    while True:
        parser = load_page(session, page_url)
        h1 = parser.find("h1")
        type_category = h1.get_text(strip=True) if h1 else None

        books = parser.find_all("article", class_="product_pod")
        for book in books:
            try:
                title = book.h3.a.get("title") or book.h3.a.get_text(strip=True)
            except Exception:
                title = None

            price_tag = book.find("p", class_="price_color")
            price = parse_price(price_tag.get_text(strip=True)) if price_tag else None

            rating = extract_rating_from_tag(book)

            img_tag = book.find("img")
            image_url = urljoin(page_url, img_tag["src"]) if img_tag and img_tag.get("src") else None

            books_data.append({
                "title": title,
                "price": price,
                "rating": rating,
                "category": type_category,
                "image": image_url,
            })

        next_a = parser.select_one("li.next a")
        if next_a and next_a.get("href"):
            next_href = next_a["href"]
            page_url = urljoin(page_url, next_href)
            time.sleep(per_page_delay)
        else:
            break

    return books_data

def fetch_image_as_base64(session: requests.Session, image_url: str, timeout: int = 20) -> str:
    """
    Faz request da imagem e retorna string base64 (utf-8). Em caso de falha, retorna "".
    """
    if not image_url:
        return ""
    try:
        resp = session.get(image_url, stream=True, timeout=timeout)
        if resp.status_code != 200:
            logger.warning("Failed to fetch image %s (status %s)", image_url, resp.status_code)
            return ""
        content = resp.content
        if not content:
            return ""
        b64 = base64.b64encode(content).decode("utf-8")
        return b64
    except Exception as exc:
        logger.exception("Exception while fetching image %s: %s", image_url, exc)
        return ""

def embed_images_as_base64(session: requests.Session, books: List[Dict],
                           delay_seconds: float = 0.4, skip_existing: bool = True):
    """
    Substitui o campo 'image' (URL) pelo conteúdo base64.
    Se skip_existing=True e o campo 'image' já parecer base64 (ex.: contém '==' ou longa),
    tenta pular para economizar requests.
    """
    for idx, b in enumerate(books, start=1):
        img_field = b.get("image") or ""
        # heuristic to detect if already a base64 string (very coarse)
        if skip_existing and isinstance(img_field, str) and len(img_field) > 200 and (img_field.endswith("==") or set(img_field) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")):
            # assume already base64, skip
            time.sleep(delay_seconds)
            continue

        b64 = fetch_image_as_base64(session, img_field, timeout=20)
        b["image"] = b64
        time.sleep(delay_seconds)

def save_books_to_csv_master(books: List[Dict], out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["title", "price", "rating", "category", "image"]
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for b in books:
            writer.writerow({
                "title": b.get("title") or "",
                "price": "" if b.get("price") is None else b.get("price"),
                "rating": "" if b.get("rating") is None else b.get("rating"),
                "category": b.get("category") or "",
                # image is base64 string (may be large)
                "image": b.get("image") or "",
            })

def scrape_category(session: requests.Session, category_href: str,
                    output_dir: Path, *, per_page_delay: float = 0.3,
                    image_delay: float = 0.4, skip_existing_images: bool = True) -> Dict:
    """
    Raspagem completa de UMA categoria (todas as páginas).
    Não salva CSV por categoria (comportamento alterado): retorna os livros com imagem em base64.
    Retorna dict com keys:
      - category_name
      - count (num livros)
      - books (list of dicts with 'image' as base64)
    """
    books = get_books(session, category_href, per_page_delay=per_page_delay)
    cat_name = books[0]["category"] if books else None

    if books:
        embed_images_as_base64(session, books, delay_seconds=image_delay, skip_existing=skip_existing_images)

    logger.info("Scraped %d books for category %s", len(books), cat_name)

    return {
        "category_name": cat_name,
        "count": len(books),
        "books": books,
    }


def scrape_all_categories(session: Optional[requests.Session] = None,
                          output_dir: Optional[str] = None,
                          *,
                          per_page_delay: float = 0.25,
                          image_delay: float = 0.4,
                          skip_existing_images: bool = True,
                          save_master_csv: bool = True,
                          max_categories: Optional[int] = None) -> Dict:
    """
    Raspagem de TODAS as categorias do site, embede imagens em base64 e grava UM CSV mestre.
    Retorna resumo dict.
    """
    session = session or create_session()
    output_dir = Path(output_dir or os.environ.get("BOOK_SCRAPER_OUTPUT", "data")).resolve()
    csv_dir = output_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting full scrape -> output: %s", output_dir)

    categories = get_categories(session)
    if max_categories:
        categories = categories[:max_categories]

    results: List[Tuple[str, List[Dict]]] = []
    total_books = 0

    for idx, (cat_name, cat_href) in enumerate(categories, start=1):
        logger.info("[%d/%d] Scraping category: %s", idx, len(categories), cat_name)
        try:
            books = get_books(session, cat_href, per_page_delay=per_page_delay)
        except Exception as exc:
            logger.exception("Error scraping category %s: %s", cat_name, exc)
            books = []

        if books:
            embed_images_as_base64(session, books, delay_seconds=image_delay, skip_existing=skip_existing_images)

        results.append((cat_name, books))
        total_books += len(books)

        time.sleep(0.55)

    # aggregate all
    all_books: List[Dict] = []
    for _cat_name, books in results:
        all_books.extend(books)

    master_csv = csv_dir / "all_books_with_images.csv"
    if save_master_csv:
        save_books_to_csv_master(all_books, master_csv)
        logger.info("Saved master CSV: %s", master_csv)

    summary = {
        "categories_count": len(results),
        "total_books": total_books,
        "output_dir": str(output_dir),
        "csv_master": str(master_csv) if save_master_csv else None,
    }
    logger.info("Scrape finished: %s", summary)
    return summary



if __name__ == "__main__":
    s = create_session()
    result = scrape_all_categories(s, output_dir="data")
    print(result)
