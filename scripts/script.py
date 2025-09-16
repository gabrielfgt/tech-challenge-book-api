import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import os
import sys

DEFAULT_CSV_PATH = "/output/books.csv"

def scrape_books(url="https://books.toscrape.com/"):
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    books_data = []
    books = soup.find_all('li', {'class': 'col-xs-6 col-sm-4 col-md-3 col-lg-3'})
    for book in books:
        title = book.h3.a["title"]
        price_text = book.find("p", class_="price_color").get_text(strip=True)
        price = float(price_text.replace("£", "").replace("Â", ""))

        link = book.h3.a["href"]
        rating = None
        for star in ['One', 'Two', 'Three', 'Four', 'Five']:
            if book.find("p", class_=f"star-rating {star}"):
                rating = star
                break

        books_data.append({
            "title": title,
            "price": price,
            "rating": rating,
            "link": urljoin(url, link)
        })
    return books_data

def save_csv(rows, out_path):
    base_dir = os.path.dirname(out_path)
    if base_dir:
        os.makedirs(base_dir, exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "price", "rating", "link"])
        writer.writeheader()
        writer.writerows(rows)

def main():
    out_path = DEFAULT_CSV_PATH if os.path.exists("/output") else "books.csv"

    books = scrape_books()
    if not books:
        print("Aviso: nenhum livro encontrado.", file=sys.stderr)

    save_csv(books, out_path)
    print(f"✅ CSV salvo em: {out_path} ({len(books)} registros)")

if __name__ == "__main__":
    main()
