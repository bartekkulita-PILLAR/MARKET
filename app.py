"""
Pillar Market V1-RAILWAY — Ceny ofertowe z OtoDom
Wersja chmurowa dla Railway.app
"""
import os
import re
import json
import math
import time
import unicodedata
from collections import deque
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# ANALITYKA
# ---------------------------------------------------------------------------
api_log = deque(maxlen=5000)
UPTIME_SINCE = datetime.utcnow().isoformat()


def log_api_call(address, status, response_ms, listings_count=0, error_message=None):
    api_log.append({
        'ts': datetime.utcnow().isoformat(),
        'address': address,
        'status': status,
        'response_ms': response_ms,
        'listings': listings_count,
        'error': error_message,
    })


# ---------------------------------------------------------------------------
# KONFIGURACJA
# ---------------------------------------------------------------------------
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'pl-PL,pl;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

FLOOR_MAP = {
    'GROUND': 0, 'FIRST': 1, 'SECOND': 2, 'THIRD': 3, 'FOURTH': 4,
    'FIFTH': 5, 'SIXTH': 6, 'SEVENTH': 7, 'EIGHTH': 8, 'NINTH': 9,
    'TENTH': 10, 'ABOVE_TENTH': 11, 'ATTIC': 12,
}
FLOOR_LABEL = {
    'GROUND': 'parter', 'FIRST': 'p.1', 'SECOND': 'p.2', 'THIRD': 'p.3',
    'FOURTH': 'p.4', 'FIFTH': 'p.5', 'SIXTH': 'p.6', 'SEVENTH': 'p.7',
    'EIGHTH': 'p.8', 'NINTH': 'p.9', 'TENTH': 'p.10',
    'ABOVE_TENTH': 'p.10+', 'ATTIC': 'poddasze',
}
ROOMS_MAP = {'ONE': 1, 'TWO': 2, 'THREE': 3, 'FOUR': 4, 'FIVE': 5,
             'SIX': 6, 'SEVEN': 7, 'MORE': 8}
ROOMS_LABEL = {'ONE': '1', 'TWO': '2', 'THREE': '3', 'FOUR': '4',
               'FIVE': '5', 'SIX': '6', 'SEVEN': '7', 'MORE': '8+'}

DISTRICT_CITY_WIDE_FALLBACK = {'praga-poludnie', 'praga-polnoc'}

# Statyczna mapa dzielnic Warszawy -> subdzielnice OtoDom
# Source of truth: otodom_warsaw_districts.json
WARSAW_DISTRICTS = {
    "bemowo": {"Jelonki Północne": "jelonki-polnocne", "Bemowo-Lotnisko": "bemowo-lotnisko", "Górce": "gorce", "Fort Bema": "fort-bema", "Fort Radiowo": "fort-radiowo", "Chrzanów": "chrzanow", "Jelonki Południowe": "jelonki-poludniowe"},
    "bialoleka": {"Brzeziny": "brzeziny", "Nowodwory": "nowodwory", "Żerań": "zeran", "Grodzisk": "grodzisk", "Kobiałka": "kobialka", "Tarchomin": "tarchomin", "Szamocin": "szamocin", "Choszczówka": "choszczowka"},
    "bielany": {"Wrzeciono": "wrzeciono", "Chomiczówka": "chomiczowka", "Radiowo": "radiowo", "Słodowiec": "slodowiec", "Sady Żoliborskie": "sady-zoliborskie", "Stare Bielany": "stare-bielany", "Huta": "huta", "Młociny": "mlociny", "Piaski": "piaski", "Marymont-Kaskada": "marymont-kaskada", "Las Bielański": "las-bielanski"},
    "mokotow": {"Siekierki": "siekierki", "Czerniaków": "czerniakow", "Ksawerów": "ksawerow", "Służewiec": "sluzewiec", "Stary Mokotów": "stary-mokotow", "Śródmieście Południowe": "srodmiescie-poludniowe", "Sielce": "sielce", "Służew": "sluzew", "Stegny": "stegny", "Wierzbno": "wierzbno", "Augustówka": "augustowka", "Wilanów Niski": "wilanow-niski"},
    "ochota": {"Filtry": "filtry", "Szczęśliwice": "szczesliwice", "Stara Ochota": "stara-ochota", "Rakowiec": "rakowiec", "Raków": "rakow"},
    "praga-poludnie": {},
    "praga-polnoc": {},
    "rembertow": {"Stary Rembertów": "stary-rembertow", "Kawęczyn-Wygoda": "kaweczyn-wygoda", "Nowy Rembertów": "nowy-rembertow"},
    "srodmiescie": {"Śródmieście Północne": "srodmiescie-polnocne", "Powiśle": "powisle", "Muranów": "muranow", "Nowolipki": "nowolipki", "Śródmieście Południowe": "srodmiescie-poludniowe", "Solec": "solec", "Stare Miasto": "stare-miasto", "Nowe Miasto": "nowe-miasto"},
    "targowek": {"Bródno": "brodno", "Zacisze": "zacisze", "Elsnerów": "elsnerow", "Targówek Fabryczny": "targowek-fabryczny", "Targówek Mieszkaniowy": "targowek-mieszkaniowy"},
    "ursus": {"Skorosze": "skorosze", "Niedźwiadek": "niedzwiadek", "Gołąbki": "golabki", "Szamoty": "szamoty"},
    "ursynow": {"Natolin": "natolin", "Kabaty": "kabaty", "Imielin": "imielin", "Stokłosy": "stoklosy", "Pyry": "pyry", "Grabów": "grabow", "Jeziorki Północne": "jeziorki-polnocne"},
    "wawer": {"Marysin Wawerski": "marysin-wawerski", "Anin": "anin", "Sadul": "sadul", "Radość": "radosc", "Międzylesie": "miedzylesie", "Falenica": "falenica", "Zerzeń": "zerzen", "Aleksandrów": "aleksandrow"},
    "wesola": {"Stara Miłosna": "stara-milosna", "Groszówka": "groszowka"},
    "wilanow": {"Błonia Wilanowskie": "blonia-wilanowskie", "Wilanów Niski": "wilanow-niski", "Zawady": "zawady", "Powsin": "powsin", "Natolin": "natolin", "Wilanów Wysoki": "wilanow-wysoki"},
    "wlochy": {"Raków": "rakow", "Stare Włochy": "stare-wlochy", "Salomea": "salomea", "Opacz Wielka": "opacz-wielka", "Nowe Włochy": "nowe-wlochy"},
    "wola": {"Czyste": "czyste", "Ulrychów": "ulrychow", "Mirów": "mirow", "Odolany": "odolany", "Młynów": "mlynow", "Koło": "kolo", "Powązki": "powazki", "Nowolipki": "nowolipki"},
    "zoliborz": {"Stary Żoliborz": "stary-zoliborz", "Żoliborz Dziennikarski": "zoliborz-dziennikarski", "Sady Żoliborskie": "sady-zoliborskie", "Plac Wilsona": "plac-wilsona", "Żoliborz Oficerski": "zoliborz-oficerski", "Marymont-Potok": "marymont-potok", "Marymont-Ruda": "marymont-ruda"},
}

# Aliasy: nazwy z Nominatim -> slug OtoDom (gdy Nominatim używa innej nazwy niż OtoDom)
NOMINATIM_TO_OTODOM_ALIASES = {
    "miasteczko-wilanow": "blonia-wilanowskie",
    "nowy-wilanow": "blonia-wilanowskie",
    "wilanow-krolewski": "wilanow-wysoki",
    "stary-imielin": "imielin",
    "krolikarnia": "stary-mokotow",
    "muranow-nowe-miasto": "muranow",
    "srodmiescie": "srodmiescie-polnocne",
    "nowe-kabaty": "kabaty",
}

CITY_PATHS = {
    'warszawa': 'mazowieckie/warszawa/warszawa/warszawa',
    'krakow': 'malopolskie/krakow/krakow/krakow',
    'wroclaw': 'dolnoslaskie/wroclaw/wroclaw/wroclaw',
    'poznan': 'wielkopolskie/poznan/poznan/poznan',
    'gdansk': 'pomorskie/gdansk/gdansk/gdansk',
    'gdynia': 'pomorskie/trojmiasto/gdynia/gdynia',
    'szczecin': 'zachodniopomorskie/szczecin/szczecin/szczecin',
    'bydgoszcz': 'kujawsko-pomorskie/bydgoszcz/bydgoszcz/bydgoszcz',
    'lublin': 'lubelskie/lublin/lublin/lublin',
    'katowice': 'slaskie/katowice/katowice/katowice',
}

BUILDING_TYPE_MAP = {
    'blok': '[BLOCK]',
    'kamienica': '[TENEMENT]',
    'apartamentowiec': '[APARTMENT]',
    'dom': '[HOUSE]',
}

MORIZON_CITY_MAP = {
    'warszawa': 'warszawa', 'krakow': 'krakow', 'wroclaw': 'wroclaw',
    'poznan': 'poznan', 'gdansk': 'gdansk', 'gdynia': 'gdynia',
    'szczecin': 'szczecin', 'bydgoszcz': 'bydgoszcz', 'lublin': 'lublin',
    'katowice': 'katowice',
}


# ---------------------------------------------------------------------------
# UTILS
# ---------------------------------------------------------------------------

def slugify(text):
    text = text.replace('ł', 'l').replace('Ł', 'L')
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# GEOCODING
# ---------------------------------------------------------------------------

def geocode_address(address):
    try:
        r = requests.get(
            'https://nominatim.openstreetmap.org/search',
            params={'q': address, 'format': 'json', 'addressdetails': 1, 'limit': 1},
            headers={'User-Agent': 'PillarMarket/1.0'},
            timeout=8
        )
        results = r.json()
        if not results:
            return None
        geo = results[0]
        addr = geo.get('address', {})
        return {
            'lat': float(geo['lat']),
            'lon': float(geo['lon']),
            'quarter': addr.get('quarter') or '',
            'neighbourhood': addr.get('neighbourhood') or '',
            'suburb': addr.get('suburb') or '',
            'city': addr.get('city') or addr.get('town') or addr.get('village') or '',
            'village': addr.get('village') or addr.get('town') or addr.get('city') or '',
            'state': addr.get('state') or '',
            'county': addr.get('county') or '',
            'municipality': addr.get('municipality') or '',
            'postcode': addr.get('postcode') or '',
        }
    except Exception:
        return None


def geocode_place(name, city):
    try:
        r = requests.get(
            'https://nominatim.openstreetmap.org/search',
            params={'q': f'{name}, {city}', 'format': 'json', 'limit': 1},
            headers={'User-Agent': 'PillarMarket/1.0'},
            timeout=6
        )
        results = r.json()
        if results:
            return float(results[0]['lat']), float(results[0]['lon'])
    except Exception:
        pass
    return None


def build_otodom_path_for_small_town(geo):
    state = re.sub(r'^województwo\s+', '', geo.get('state', ''), flags=re.I).strip()
    county = re.sub(r'^powiat\s+', '', geo.get('county', ''), flags=re.I).strip()
    municipality = re.sub(r'^gmina\s+', '', geo.get('municipality', ''), flags=re.I).strip()
    village = geo.get('village', '') or geo.get('city', '')
    city = geo.get('city', '')

    vs = slugify(state)
    vc = slugify(county)
    vm = slugify(municipality)
    vv = slugify(village)
    vcity = slugify(city)

    if vs and vc and vm and vv:
        return f'{vs}/{vc}/{vm}/{vv}'
    if vs and vc and vv:
        return f'{vs}/{vc}/{vv}'
    # Miasta na prawach powiatu (Płock, Łódź itp.) - brak county/municipality w Nominatim
    if vs and vcity and not vc and not vm:
        return f'{vs}/{vcity}/{vcity}/{vcity}'
    return None


# ---------------------------------------------------------------------------
# OTODOM
# ---------------------------------------------------------------------------

def get_otodom_subdistricts(city_path, district_slug):
    url = f'https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/{city_path}/{district_slug}?viewType=listing&limit=36'
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return {}
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.DOTALL)
        if not m:
            return {}
        d = json.loads(m.group(1))
        items = d['props']['pageProps']['data']['searchAds']['items']
        result = {}
        for item in items:
            locs = (item.get('location', {}).get('reverseGeocoding') or {}).get('locations', [])
            for loc in locs:
                if loc.get('locationLevel') == 'residential':
                    name = loc['name']
                    if name and name not in result:
                        result[name] = slugify(name)
        return result
    except Exception:
        return {}


def find_otodom_area(city_slug, quarter, suburb, neighbourhood='', lat=None, lon=None):
    city_path = CITY_PATHS.get(city_slug)
    if not city_path:
        return None, None

    district_slug = slugify(suburb) if suburb else None
    if not district_slug:
        return None, None

    # Dla Warszawy: uzyj statycznej mapy (stabilna, nie zalezy od scrapowania)
    if city_slug == 'warszawa' and district_slug in WARSAW_DISTRICTS:
        subdistricts = WARSAW_DISTRICTS[district_slug]
    else:
        subdistricts = get_otodom_subdistricts(city_path, district_slug)

    if not subdistricts:
        return district_slug, None

    subdistrict_slugs = set(subdistricts.values())

    # 1. Bezposrednie dopasowanie nazwy z Nominatim
    for candidate in [quarter, neighbourhood]:
        if candidate:
            s = slugify(candidate)
            if s in subdistrict_slugs:
                return district_slug, s
            # 2. Sprawdz aliasy (Nominatim -> OtoDom)
            alias = NOMINATIM_TO_OTODOM_ALIASES.get(s)
            if alias and alias in subdistrict_slugs:
                return district_slug, alias

    # 3. Fallback: geocoding najblizszej subdzielnicy
    if lat and lon:
        city_name = suburb or city_slug
        best_slug = None
        best_dist = float('inf')
        for name, slug in subdistricts.items():
            coords = geocode_place(name, city_name)
            if coords:
                dist = haversine(lat, lon, coords[0], coords[1])
                if dist < best_dist:
                    best_dist = dist
                    best_slug = slug
        if best_slug:
            return district_slug, best_slug

    return district_slug, None


def fetch_otodom_listings(city_slug, district_slug, residential_slug, rooms, area_min, area_max, pages=3, building_type='all', location_path_override=None, district_name_filter=None):
    if location_path_override:
        location_path = location_path_override
    else:
        city_path = CITY_PATHS.get(city_slug, 'mazowieckie/warszawa/warszawa/warszawa')
        if residential_slug:
            location_path = f'{city_path}/{district_slug}/{residential_slug}'
        elif district_slug and district_slug not in DISTRICT_CITY_WIDE_FALLBACK:
            location_path = f'{city_path}/{district_slug}'
        else:
            location_path = city_path

    is_house = building_type == 'dom'
    estate_type = 'dom' if is_house else 'mieszkanie'

    if is_house or str(rooms).upper() == 'ALL':
        rooms_url_segment = estate_type
    else:
        rooms_param = rooms.upper() if isinstance(rooms, str) else {
            1: 'ONE', 2: 'TWO', 3: 'THREE', 4: 'FOUR', 5: 'FIVE'
        }.get(int(rooms), 'TWO')
        rooms_url_segment = f'mieszkanie,{rooms_param.lower()}-pokoje' if rooms_param != 'ONE' else 'mieszkanie,1-pokoj'

    otodom_bt = BUILDING_TYPE_MAP.get(building_type, '')
    building_type_param = f'&buildingType=%5B{otodom_bt[1:-1]}%5D' if otodom_bt and not is_house else ''

    all_items = []
    for page in range(1, pages + 1):
        url = (
            f'https://www.otodom.pl/pl/wyniki/sprzedaz/{rooms_url_segment}/'
            f'{location_path}'
            f'?areaMin={area_min}&areaMax={area_max}'
            f'{building_type_param}'
            f'&market=SECONDARY'
            f'&viewType=listing&limit=36&page={page}'
        )
        try:
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.status_code != 200:
                break
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.DOTALL)
            if not m:
                break
            d = json.loads(m.group(1))
            items = d['props']['pageProps']['data']['searchAds']['items']
            total_pages = d['props']['pageProps']['data']['searchAds']['pagination']['totalPages']
            all_items.extend(items)
            if page >= total_pages:
                break
        except Exception:
            break

    if district_name_filter and all_items:
        def item_district(item):
            locs = (item.get('location', {}).get('reverseGeocoding') or {}).get('locations', [])
            return next((l['name'] for l in locs if l.get('locationLevel') == 'district'), '')
        all_items = [it for it in all_items if district_name_filter.lower() in item_district(it).lower()]

    return all_items, location_path


def parse_listing(item):
    price = (item.get('totalPrice') or {}).get('value', 0) or 0
    ppm2 = (item.get('pricePerSquareMeter') or {}).get('value', 0) or 0
    area = item.get('areaInSquareMeters') or 0
    floor_raw = item.get('floorNumber', '') or ''
    rooms_raw = item.get('roomsNumber', '') or ''
    slug = item.get('slug', '') or ''
    date_str = item.get('createdAtFirst') or item.get('dateCreated') or ''
    images = item.get('images') or []
    image_url = images[0].get('medium', '') if images else ''

    loc = item.get('location', {}) or {}
    adr = loc.get('address', {}) or {}
    street_obj = adr.get('street') or {}
    street = street_obj.get('name', '') if isinstance(street_obj, dict) else ''
    number = street_obj.get('number', '') if isinstance(street_obj, dict) else ''
    locs2 = (loc.get('reverseGeocoding') or {}).get('locations', []) or []
    subdist = next((l['name'] for l in locs2 if l.get('locationLevel') == 'residential'), '')
    district = next((l['name'] for l in locs2 if l.get('locationLevel') == 'district'), '')
    city_name = next((l['name'] for l in locs2 if l.get('locationLevel') == 'city_or_village'), '') or (adr.get('city') or {}).get('name', '')

    if street and number:
        full_addr = f'{street} {number}'
    elif street:
        full_addr = street
    elif subdist:
        full_addr = subdist
    elif district:
        full_addr = district
    else:
        full_addr = '-'

    date_obj = None
    for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            date_obj = datetime.strptime(date_str[:19], fmt)
            break
        except (ValueError, TypeError):
            continue

    return {
        'price': int(price),
        'ppm2': int(ppm2),
        'area': float(area),
        'floor_raw': floor_raw,
        'floor_num': FLOOR_MAP.get(floor_raw, 5),
        'floor_label': FLOOR_LABEL.get(floor_raw, floor_raw or '?'),
        'rooms_raw': rooms_raw,
        'rooms_num': ROOMS_MAP.get(rooms_raw, 2),
        'rooms_label': ROOMS_LABEL.get(rooms_raw, '?'),
        'address': full_addr,
        'city': city_name,
        'date': date_obj.strftime('%Y-%m-%d') if date_obj else date_str[:10],
        'date_obj': date_obj,
        'link': f'https://www.otodom.pl/pl/oferta/{slug}' if slug else '',
        'title': item.get('title', ''),
        'image': image_url,
    }


# ---------------------------------------------------------------------------
# MORIZON
# ---------------------------------------------------------------------------

def fetch_morizon_listings(city_slug, district_slug, rooms, area_min, area_max, pages=2):
    city = MORIZON_CITY_MAP.get(city_slug)
    if not city or not district_slug:
        return []

    all_items = []
    rooms_param = f'&ps%5Bnumber_of_rooms_from%5D={rooms}&ps%5Bnumber_of_rooms_to%5D={rooms}' if str(rooms) != 'all' else ''

    url = (
        f'https://www.morizon.pl/mieszkania/sprzedaz/{city}/{district_slug}/'
        f'?ps%5Bsize_from%5D={area_min}&ps%5Bsize_to%5D={area_max}'
        f'{rooms_param}'
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200:
            scripts = re.findall(r'<script[^>]*>(.*?)</script>', r.text, re.DOTALL)
            for s in scripts:
                try:
                    d = json.loads(s.strip())
                    if isinstance(d, dict) and d.get('@type') == 'Product' and 'offers' in d:
                        all_items.extend(d['offers']['offers'])
                        break
                except Exception:
                    continue
    except Exception:
        pass

    return all_items


def parse_morizon_listing(offer):
    io = offer.get('itemOffered', {})
    price = int(float(offer.get('price', 0) or 0))
    area = float((io.get('floorSize') or {}).get('value', 0) or 0)
    ppm2 = int(price / area) if area > 0 else 0
    rooms_num = int(io.get('numberOfRooms', 0) or 0)
    floor_num = int(io.get('floorLevel', 0) or 0)
    addr = io.get('address', {})
    street = addr.get('streetAddress', '') or ''
    morizon_city = addr.get('addressLocality', '') or ''
    image = offer.get('image', '') or ''
    link = offer.get('url', '') or ''
    title = offer.get('name', '') or ''

    return {
        'price': price,
        'ppm2': ppm2,
        'area': area,
        'floor_raw': str(floor_num),
        'floor_num': floor_num,
        'floor_label': f'p.{floor_num}' if floor_num > 0 else 'parter',
        'rooms_raw': str(rooms_num),
        'rooms_num': rooms_num,
        'rooms_label': str(rooms_num),
        'address': street or '-',
        'city': morizon_city,
        'date': '',
        'date_obj': None,
        'link': link,
        'title': title,
        'image': image,
        'source': 'morizon',
    }


# ---------------------------------------------------------------------------
# PROCESSING
# ---------------------------------------------------------------------------

def dedup_cross_portal(items):
    seen = {}
    result = []
    for it in items:
        price_key = round(it['price'] / 5000)
        area_key = round(it['area'])
        rooms_key = it['rooms_num']
        key = (price_key, area_key, rooms_key)
        if key in seen and it['price'] > 0 and it['area'] > 0:
            existing = result[seen[key]]
            if 'alt_links' not in existing:
                existing['alt_links'] = []
            existing['alt_links'].append({'source': it.get('source', '?'), 'link': it['link']})
        else:
            seen[key] = len(result)
            if 'source' not in it:
                it['source'] = 'otodom'
            result.append(it)
    return result


def remove_duplicates(items):
    seen = set()
    result = []
    for it in items:
        key = it['link']
        if key and key not in seen:
            seen.add(key)
            result.append(it)
    return result


def remove_outliers(items):
    ppms = sorted([it['ppm2'] for it in items if it['ppm2'] > 0])
    if not ppms:
        return items
    median = ppms[len(ppms) // 2]
    return [it for it in items if it['ppm2'] <= median * 2.2 and it['ppm2'] > 0]


def score_listing(listing, target_area, target_rooms, target_floor, target_street=None):
    if listing['area'] > 0:
        area_diff = abs(listing['area'] - target_area) / target_area
        area_score = max(0.0, 1.0 - area_diff * 3.0)
    else:
        area_score = 0.0

    rooms_diff = abs(listing['rooms_num'] - target_rooms)
    rooms_score = max(0.0, 1.0 - rooms_diff * 0.4)

    floor_diff = abs(listing['floor_num'] - target_floor)
    floor_score = max(0.0, 1.0 - floor_diff * 0.2)

    if listing['date_obj']:
        now = datetime.now()
        days_old = (now - listing['date_obj']).days
        recency_score = max(0.0, 1.0 - days_old / 365.0)
    else:
        recency_score = 0.5

    total = (area_score * 0.50 + rooms_score * 0.25 +
             floor_score * 0.15 + recency_score * 0.10)
    base = round(total * 100)

    street_match = False
    if target_street and listing.get('address'):
        ts = slugify(target_street)
        ls = slugify(listing['address'])
        street_match = ts and ts in ls
    listing['street_match'] = street_match

    return base + (50 if street_match else 0)


# ---------------------------------------------------------------------------
# FLASK ROUTES
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search():
    data = request.get_json()
    address = data.get('address', '').strip()
    house_number = data.get('house_number', '').strip()
    area = float(data.get('area') or 0)
    rooms = int(data.get('rooms') or 0)
    floor = int(data.get('floor') or 0)
    building_type = data.get('building_type', 'all')
    if not area:
        return jsonify({'error': 'Podaj metraż mieszkania.'}), 400
    if not rooms and building_type != 'dom':
        return jsonify({'error': 'Wybierz liczbę pokoi.'}), 400
    area_margin = float(data.get('area_margin', 0.25))

    if ',' in address:
        parts = address.rsplit(',', 1)
        street_only = parts[0].strip()
        city_from_input = parts[1].strip()
    else:
        street_only = address
        city_from_input = 'Warszawa'
    full_address = f'{street_only} {house_number}, {city_from_input}' if house_number else f'{street_only}, {city_from_input}'

    t0 = time.time()

    geo = geocode_address(full_address)
    if not geo:
        elapsed_ms = round((time.time() - t0) * 1000)
        log_api_call(full_address, 'geocode_fail', elapsed_ms, 0, 'Nie znaleziono adresu')
        return jsonify({'error': 'Nie udało się zlokalizować adresu. Sprawdź pisownię.'}), 400

    city_raw = geo['city'].lower()
    city_slug = slugify(city_raw)
    is_major_city = city_slug in CITY_PATHS

    area_min = int(area * (1 - area_margin))
    area_max = int(area * (1 + area_margin))
    rooms_str = 'all' if building_type == 'dom' or not rooms else str(rooms)

    if is_major_city:
        quarter = geo['quarter']
        neighbourhood = geo['neighbourhood']
        suburb = geo['suburb']
        district_slug, residential_slug = find_otodom_area(
            city_slug, quarter, suburb, neighbourhood,
            lat=geo['lat'], lon=geo['lon']
        )
        if not district_slug:
            district_slug = slugify(suburb) if suburb else None
        effective_residential = None if building_type == 'dom' else residential_slug
        loc_override = None
        district_name_filter = suburb if district_slug in DISTRICT_CITY_WIDE_FALLBACK else None
    else:
        district_slug = None
        residential_slug = None
        effective_residential = None
        district_name_filter = None
        loc_override = build_otodom_path_for_small_town(geo)
        if not loc_override:
            elapsed_ms = round((time.time() - t0) * 1000)
            log_api_call(full_address, 'error', elapsed_ms, 0, 'Brak ścieżki OtoDom')
            return jsonify({'error': f'Nie udało się znaleźć lokalizacji "{geo["city"]}" w OtoDom.'}), 400

    raw_items, location_path = fetch_otodom_listings(
        city_slug, district_slug, effective_residential,
        rooms_str, area_min, area_max, pages=3, building_type=building_type,
        location_path_override=loc_override, district_name_filter=district_name_filter
    )

    area_min2 = int(area * 0.5)
    area_max2 = int(area * 1.5)
    if len(raw_items) < 8:
        raw_items2, _ = fetch_otodom_listings(
            city_slug, district_slug, effective_residential,
            rooms_str, area_min2, area_max2, pages=3, building_type=building_type,
            location_path_override=loc_override, district_name_filter=district_name_filter
        )
        if len(raw_items2) > len(raw_items):
            raw_items = raw_items2

    if len(raw_items) < 8:
        raw_items3, _ = fetch_otodom_listings(
            city_slug, district_slug, effective_residential,
            'all', area_min2, area_max2, pages=3, building_type=building_type,
            location_path_override=loc_override, district_name_filter=district_name_filter
        )
        if len(raw_items3) > len(raw_items):
            raw_items = raw_items3

    if not raw_items:
        elapsed_ms = round((time.time() - t0) * 1000)
        log_api_call(full_address, 'empty', elapsed_ms, 0)
        return jsonify({'error': f'Brak ofert dla tego obszaru ({location_path}). Spróbuj zwiększyć margines metrażu.'}), 404

    listings = [parse_listing(it) for it in raw_items]
    listings = remove_duplicates(listings)

    morizon_raw = fetch_morizon_listings(city_slug, district_slug, rooms_str, area_min, area_max, pages=2)
    if len(morizon_raw) < 5:
        morizon_raw = fetch_morizon_listings(city_slug, district_slug, 'all', int(area * 0.5), int(area * 1.5), pages=2)
    morizon_listings = [parse_morizon_listing(o) for o in morizon_raw]
    listings = listings + morizon_listings

    listings = dedup_cross_portal(listings)
    listings = remove_outliers(listings)

    for lst in listings:
        lst['score'] = score_listing(lst, area, rooms, floor, target_street=street_only)

    listings.sort(key=lambda x: x['score'], reverse=True)
    listings = listings[:30]

    ppms = sorted([it['ppm2'] for it in listings if it['ppm2'] > 0])
    prices = [it['price'] for it in listings if it['price'] > 0]
    median_ppm2 = ppms[len(ppms) // 2] if ppms else 0
    estimated_price = int(area * median_ppm2)

    area_label = f'{residential_slug or district_slug or city_slug}'.replace('-', ' ').title()
    street_matches = sum(1 for l in listings if l.get('street_match'))

    elapsed_ms = round((time.time() - t0) * 1000)
    log_api_call(full_address, 'ok', elapsed_ms, len(listings))

    return jsonify({
        'listings': listings,
        'meta': {
            'area_label': area_label,
            'location_path': location_path,
            'target_street': street_only,
            'street_matches': street_matches,
            'total': len(listings),
            'median_ppm2': median_ppm2,
            'min_ppm2': min(ppms) if ppms else 0,
            'max_ppm2': max(ppms) if ppms else 0,
            'min_price': min(prices) if prices else 0,
            'max_price': max(prices) if prices else 0,
            'estimated_price': estimated_price,
            'query': {
                'address': full_address,
                'area': area,
                'rooms': rooms,
                'floor': floor,
                'area_min': area_min,
                'area_max': area_max,
            }
        }
    })


# ---------------------------------------------------------------------------
# API SEARCH (GET) - dla pipeline
# ---------------------------------------------------------------------------

@app.route('/api/search')
def api_search():
    address = request.args.get('address', '').strip()
    area = float(request.args.get('area', 0))
    rooms_raw = request.args.get('rooms', '0')
    try:
        rooms = int(rooms_raw)
    except ValueError:
        rooms = 0
    floor = int(request.args.get('floor', 0))
    building_type = request.args.get('building_type', 'all')

    if not address or not area:
        return jsonify({'error': 'Wymagane parametry: address, area'}), 400

    return search_internal(address, '', area, rooms, floor, building_type)


def search_internal(address, house_number, area, rooms, floor, building_type):
    if ',' in address:
        parts = address.rsplit(',', 1)
        street_only = parts[0].strip()
        city_from_input = parts[1].strip()
    else:
        street_only = address
        city_from_input = 'Warszawa'
    full_address = f'{street_only} {house_number}, {city_from_input}' if house_number else f'{street_only}, {city_from_input}'

    t0 = time.time()

    geo = geocode_address(full_address)
    if not geo:
        elapsed_ms = round((time.time() - t0) * 1000)
        log_api_call(full_address, 'geocode_fail', elapsed_ms, 0)
        return jsonify({'error': 'Nie znaleziono adresu'}), 404

    city_raw = geo['city'].lower()
    city_slug = slugify(city_raw)
    is_major_city = city_slug in CITY_PATHS

    area_min = int(area * 0.75)
    area_max = int(area * 1.25)
    rooms_str = 'all' if building_type == 'dom' or not rooms else str(rooms)

    if is_major_city:
        district_slug, residential_slug = find_otodom_area(
            city_slug, geo['quarter'], geo['suburb'], geo['neighbourhood'],
            lat=geo['lat'], lon=geo['lon']
        )
        if not district_slug:
            district_slug = slugify(geo['suburb']) if geo['suburb'] else None
        effective_residential = None if building_type == 'dom' else residential_slug
        loc_override = None
        district_name_filter = geo['suburb'] if district_slug in DISTRICT_CITY_WIDE_FALLBACK else None
    else:
        district_slug = None
        residential_slug = None
        effective_residential = None
        district_name_filter = None
        loc_override = build_otodom_path_for_small_town(geo)

    # Krok 1: standardowe parametry (area +-25%, pages=3)
    raw_items, location_path = fetch_otodom_listings(
        city_slug, district_slug, effective_residential,
        rooms_str, area_min, area_max, pages=3, building_type=building_type,
        location_path_override=loc_override, district_name_filter=district_name_filter
    )

    # Krok 2: fallback - rozszerzony metraz (area +-50%) jesli <8 ofert
    area_min2 = int(area * 0.5)
    area_max2 = int(area * 1.5)
    if len(raw_items) < 8:
        raw_items2, _ = fetch_otodom_listings(
            city_slug, district_slug, effective_residential,
            rooms_str, area_min2, area_max2, pages=3, building_type=building_type,
            location_path_override=loc_override, district_name_filter=district_name_filter
        )
        if len(raw_items2) > len(raw_items):
            raw_items = raw_items2

    # Krok 3: fallback - bez filtra pokoi jesli nadal <8 ofert
    if len(raw_items) < 8:
        raw_items3, _ = fetch_otodom_listings(
            city_slug, district_slug, effective_residential,
            'all', area_min2, area_max2, pages=3, building_type=building_type,
            location_path_override=loc_override, district_name_filter=district_name_filter
        )
        if len(raw_items3) > len(raw_items):
            raw_items = raw_items3

    listings = [parse_listing(it) for it in raw_items]
    listings = remove_duplicates(listings)

    # Morizon - dodatkowe zrodlo, ale FILTRUJ po miescie
    city_name_lower = city_from_input.lower()
    morizon_raw = fetch_morizon_listings(city_slug, district_slug, rooms_str, area_min, area_max, pages=2)
    if len(morizon_raw) < 5:
        morizon_raw = fetch_morizon_listings(city_slug, district_slug, 'all', area_min2, area_max2, pages=2)
    morizon_listings = [parse_morizon_listing(o) for o in morizon_raw]
    morizon_listings = [m for m in morizon_listings if m.get('city', '').lower() in (city_name_lower, city_slug, '') or city_name_lower in m.get('city', '').lower()]
    listings = listings + morizon_listings

    listings = dedup_cross_portal(listings)
    listings = remove_outliers(listings)

    for lst in listings:
        lst['score'] = score_listing(lst, area, rooms, floor, target_street=street_only)
    listings.sort(key=lambda x: x['score'], reverse=True)
    listings = listings[:20]

    ppms = sorted([it['ppm2'] for it in listings if it['ppm2'] > 0])
    median_ppm2 = ppms[len(ppms) // 2] if ppms else 0

    elapsed_ms = round((time.time() - t0) * 1000)
    log_api_call(full_address, 'ok' if listings else 'empty', elapsed_ms, len(listings))

    return jsonify({
        'listings': listings,
        'meta': {
            'total': len(listings),
            'median_ppm2': median_ppm2,
            'estimated_price': int(area * median_ppm2),
        },
        'response_ms': elapsed_ms,
    })


# ---------------------------------------------------------------------------
# STATS & HEALTH
# ---------------------------------------------------------------------------

@app.route('/api/stats')
def api_stats():
    now = datetime.utcnow()
    cutoff_24h = (now - timedelta(hours=24)).isoformat()
    all_entries = list(api_log)
    last_24h = [e for e in all_entries if e['ts'] >= cutoff_24h]

    def calc(entries):
        total = len(entries)
        ok = sum(1 for e in entries if e['status'] == 'ok')
        error = sum(1 for e in entries if e['status'] == 'error')
        empty = sum(1 for e in entries if e['status'] == 'empty')
        times = [e['response_ms'] for e in entries if e['response_ms']]
        return {
            'total': total, 'ok': ok, 'error': error, 'empty': empty,
            'avg_response_ms': round(sum(times) / len(times)) if times else 0,
        }

    result = calc(all_entries)
    result['last_24h'] = calc(last_24h)
    result['uptime_since'] = UPTIME_SINCE
    return jsonify(result)


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'version': '1.0-railway'})


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8383))
    app.run(debug=False, port=port, host='0.0.0.0')
