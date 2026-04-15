#!/usr/bin/env python3
"""
Générateur de flux XML produits multi-plateforme pour Nuhanciam.
Version corrigée pour une structure XML valide.
"""

import json
import os
import sys
import argparse
import logging
import re
from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom.minidom import parseString
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Configuration & Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("nuhanciam_feed")

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

def load_config(path: str = DEFAULT_CONFIG_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# Shopify API Client
class ShopifyClient:
    API_VERSION = "2024-01"

    def __init__(self, shop_domain: str, access_token: str):
        self.base_url = f"https://{shop_domain}/admin/api/{self.API_VERSION}"
        self.access_token = access_token

    def _get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        url = f"{self.base_url}/{endpoint}.json"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query}"

        req = Request(url, headers={
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        })
        try:
            with urlopen(req) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (HTTPError, URLError) as e:
            logger.error(f"Shopify API error: {e}")
            raise

    def get_products(self, limit: int = 250) -> list:
        all_products = []
        params = {"limit": limit, "published_status": "published"}
        
        while True:
            data = self._get("products", params)
            products = data.get("products", [])
            all_products.extend(products)
            if len(products) < limit:
                break
            params["since_id"] = products[-1]["id"]
            
        logger.info(f"Récupéré {len(all_products)} produits")
        return all_products

# Mappers
def clean_html(html_text: str) -> str:
    if not html_text: return ""
    clean = re.sub(r"<[^>]+>", "", html_text)
    return re.sub(r"\s+", " ", clean).strip()

def get_availability(variant: dict) -> str:
    if variant.get("inventory_management") is None: return "in_stock"
    qty = variant.get("inventory_quantity", 0)
    policy = variant.get("inventory_policy", "deny")
    return "in_stock" if qty > 0 or policy == "continue" else "out_of_stock"

def map_google_category(product_type: str, tags: list) -> str:
    default = "Health & Beauty > Skin Care"
    mappings = {
        "sérum": "Health & Beauty > Skin Care > Facial Skin Care > Face Serums",
        "crème": "Health & Beauty > Skin Care > Facial Skin Care > Facial Moisturizers",
        "corps": "Health & Beauty > Skin Care > Body Skin Care > Body Lotions & Creams",
    }
    type_l = (product_type or "").lower()
    for k, v in mappings.items():
        if k in type_l: return v
    return default

def build_item(product: dict, variant: dict, config: dict) -> dict:
    shop_url = config["shop_url"].rstrip("/")
    tags = [t.strip() for t in (product.get("tags") or "").split(",") if t.strip()]
    
    price = f"{variant.get('price')} {config.get('currency', 'EUR')}"
    sale_price = ""
    comp = variant.get("compare_at_price")
    if comp and float(comp) > float(variant.get("price", 0)):
        sale_price = price
        price = f"{comp} {config.get('currency', 'EUR')}"

    images = product.get("images", [])
    img_link = images[0]["src"] if images else ""
    additional = [img["src"] for img in images[1:11]]

    return {
        "g:id": variant.get("sku") or f"sh_{variant['id']}",
        "g:title": product.get("title"),
        "g:description": clean_html(product.get("body_html"))[:5000],
        "g:link": f"{shop_url}/products/{product['handle']}?variant={variant['id']}",
        "g:image_link": img_link,
        "g:additional_image_link": additional,
        "g:availability": get_availability(variant),
        "g:price": price,
        "g:sale_price": sale_price,
        "g:brand": config.get("brand", "Nuhanciam"),
        "g:condition": "new",
        "g:google_product_category": map_google_category(product.get("product_type"), tags),
        "g:gtin": variant.get("barcode") or "",
        "g:item_group_id": str(product["id"]),
        "g:shipping_weight": f"{variant.get('weight')} {variant.get('weight_unit', 'kg')}" if variant.get("weight") else ""
    }

# Génération XML
def generate_xml_feed(products: list, config: dict) -> str:
    # Définition des Namespaces
    namespaces = {
        "version": "2.0",
        "xmlns:g": "http://base.google.com/ns/1.0",
        "xmlns:atom": "http://www.w3.org/2005/Atom",
    }
    rss = Element("rss", attrib=namespaces)
    channel = SubElement(rss, "channel")

    SubElement(channel, "title").text = config.get("feed_title")
    SubElement(channel, "link").text = config.get("shop_url")
    SubElement(channel, "description").text = config.get("feed_description")

    for product in products:
        for variant in product.get("variants", []):
            item_data = build_item(product, variant, config)
            item_el = SubElement(channel, "item")
            
            for key, value in item_data.items():
                if not value: continue
                
                if key == "g:additional_image_link" and isinstance(value, list):
                    for img in value:
                        SubElement(item_el, key).text = img
                else:
                    SubElement(item_el, key).text = str(value)

    # Conversion en chaîne propre
    xml_str = tostring(rss, encoding="utf-8")
    dom = parseString(xml_str)
    return dom.toprettyxml(indent="  ")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--output", default="feed.xml")
    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except Exception:
        config = {
            "shop_domain": "nuhanciam.myshopify.com",
            "shop_url": "https://nuhanciam.com",
            "brand": "Nuhanciam",
            "currency": "EUR",
            "feed_title": "Nuhanciam Catalog"
        }

    shop_domain = config.get("shop_domain")
    access_token = config.get("access_token") or os.environ.get("SHOPIFY_ACCESS_TOKEN")

    if not access_token:
        print("Erreur: access_token manquant.")
        sys.exit(1)

    client = ShopifyClient(shop_domain, access_token)
    products = client.get_products()
    xml_content = generate_xml_feed(products, config)

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(xml_content)
    
    print(f"Succès: Flux généré dans {args.output}")

if __name__ == "__main__":
    main()