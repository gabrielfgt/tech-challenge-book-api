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
    """
    Retorna lista de (category_name, category_href) onde href pode ser relativo.
    """
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
    Cada livro: dict { title, price, rating, category, image, image_local (None) }.
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
                "image_local": None,
            })

        next_a = parser.select_one("li.next a")
        if next_a and next_a.get("href"):
            next_href = next_a["href"]
            page_url = urljoin(page_url, next_href)
            time.sleep(per_page_delay)
        else:
            break

    return books_data


def download_image(session: requests.Session, image_url: str, dest_path: Path,
                   timeout: int = 20) -> bool:
    try:
        resp = session.get(image_url, stream=True, timeout=timeout)
        if resp.status_code != 200:
            logger.warning("Failed to download %s (status %s)", image_url, resp.status_code)
            return False

        ext = dest_path.suffix
        if not ext:
            ext = get_extension_from_url_or_ct(image_url, resp)
            dest_path = dest_path.with_suffix(ext)

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as exc:
        logger.exception("Exception while downloading %s: %s", image_url, exc)
        return False

def download_and_update_books_images(session: requests.Session, books: List[Dict],
                                     images_root: Path, delay_seconds: float = 0.5,
                                     skip_existing: bool = True):
    images_root = Path(images_root)
    for idx, b in enumerate(books, start=1):
        img_url = b.get("image")
        if not img_url:
            b["image_local"] = ""
            continue

        cat_slug = safe_slug(b.get("category") or "unknown")
        title_slug = safe_slug(b.get("title") or f"book-{idx}", maxlen=80)
        short_hash = hashlib.sha1(img_url.encode("utf-8")).hexdigest()[:8]
        path = urlparse(img_url).path
        ext = Path(path).suffix or ""
        filename = f"{title_slug}-{short_hash}{ext}"
        dest_rel = Path(cat_slug) / filename
        dest = images_root / dest_rel

        if skip_existing and dest.exists():
            b["image_local"] = str(dest.as_posix())
            time.sleep(delay_seconds)
            continue

        success = download_image(session, img_url, dest)
        b["image_local"] = str(dest.as_posix()) if success else ""
        time.sleep(delay_seconds)

def save_books_to_csv(books: List[Dict], out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["title", "price", "rating", "category", "image", "image_local"]
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for b in books:
            writer.writerow({
                "title": b.get("title") or "",
                "price": "" if b.get("price") is None else b.get("price"),
                "rating": "" if b.get("rating") is None else b.get("rating"),
                "category": b.get("category") or "",
                "image": b.get("image") or "",
                "image_local": b.get("image_local") or "",
            })

def scrape_category(session: requests.Session, category_href: str,
                    output_dir: Path, *, per_page_delay: float = 0.3,
                    image_delay: float = 0.4, skip_existing_images: bool = True) -> Dict:
    """
    Raspagem completa de UMA categoria (todas as páginas).
    Retorna dict com keys:
      - category_name
      - count (num livros)
      - csv_path (Path)
      - images_dir (Path)
      - books (list of dicts)
    """
    books = get_books(session, category_href, per_page_delay=per_page_delay)
    cat_name = books[0]["category"] if books else None
    safe_name = safe_slug(cat_name or "unknown-category")

    images_dir = Path(output_dir) / "images"
    csv_dir = Path(output_dir) / "csv"
    per_cat_csv = csv_dir / f"{safe_name}.csv"

    if books:
        download_and_update_books_images(session, books, images_dir,
                                         delay_seconds=image_delay, skip_existing=skip_existing_images)

    save_books_to_csv(books, per_cat_csv)
    logger.info("Saved %d books for category %s -> %s", len(books), cat_name, per_cat_csv)

    return {
        "category_name": cat_name,
        "count": len(books),
        "csv_path": per_cat_csv,
        "images_dir": images_dir / safe_name,
        "books": books,
    }


def scrape_all_categories(session: Optional[requests.Session] = None,
                          output_dir: Optional[str] = None,
                          *,
                          per_page_delay: float = 0.25,
                          image_delay: float = 0.4,
                          skip_existing_images: bool = True,
                          save_per_category_csv: bool = True,
                          save_master_csv: bool = True,
                          max_categories: Optional[int] = None) -> Dict:
    """
    Raspagem de TODAS as categorias do site.
    Retorna um resumo (dict) com estatísticas e caminhos de arquivos produzidos.

    Parâmetros:
     - session: requests.Session (se None, será criado)
     - output_dir: pasta onde serão salvos os dados (default: "./data")
     - per_page_delay: delay entre páginas da mesma categoria
     - image_delay: delay entre downloads de imagens
     - skip_existing_images: se True evita rebaixar imagens já existentes
     - save_per_category_csv: se True cria csv por categoria
     - save_master_csv: se True cria CSV mestre com todas as categorias
     - max_categories: se setado, limita quantas categorias varrer (útil em testes)
    """
    session = session or create_session()
    output_dir = Path(output_dir or os.environ.get("BOOK_SCRAPER_OUTPUT", "data")).resolve()
    images_dir = output_dir / "images"
    csv_dir = output_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    images_dir.mkdir(parents=True, exist_ok=True)

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
            download_and_update_books_images(session, books, images_dir,
                                             delay_seconds=image_delay, skip_existing=skip_existing_images)

        
        safe_name = safe_slug(cat_name or f"cat-{idx}", maxlen=60)
        per_cat_csv = csv_dir / f"{safe_name}.csv"
        if save_per_category_csv:
            save_books_to_csv(books, per_cat_csv)
            logger.info("Saved category CSV: %s", per_cat_csv)

        results.append((cat_name, books))
        total_books += len(books)

        time.sleep(0.55)

    all_books = []
    for _cat_name, books in results:
        all_books.extend(books)

    master_csv = csv_dir / "all_books_with_images.csv"
    if save_master_csv:
        save_books_to_csv(all_books, master_csv)
        logger.info("Saved master CSV: %s", master_csv)

    summary = {
        "categories_count": len(results),
        "total_books": total_books,
        "output_dir": str(output_dir),
        "csv_master": str(master_csv) if save_master_csv else None,
        "csv_dir": str(csv_dir),
        "images_dir": str(images_dir),
    }
    logger.info("Scrape finished: %s", summary)
    return summary



if __name__ == "__main__":
    s = create_session()
    result = scrape_all_categories(s, output_dir="data")
    print(result)
