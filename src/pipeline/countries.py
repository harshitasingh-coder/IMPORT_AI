"""Small country table: name/alias -> ISO3, UN M49 (Comtrade) code, ccTLD."""

# iso3: (name, comtrade_code, cctld, aliases)
_COUNTRIES = {
    "IND": ("India", 699, "in", ["republic of india", "bharat", "in", "ind"]),
    "IDN": ("Indonesia", 360, "id", ["republic of indonesia", "id", "idn"]),
    "VNM": ("Viet Nam", 704, "vn", ["vietnam", "vn", "vnm", "socialist republic of vietnam"]),
    "NLD": ("Netherlands", 528, "nl", ["the netherlands", "holland", "nl", "nld"]),
    "MYS": ("Malaysia", 458, "my", ["my", "mys"]),
    "THA": ("Thailand", 764, "th", ["th", "tha"]),
    "PHL": ("Philippines", 608, "ph", ["the philippines", "ph", "phl"]),
    "SGP": ("Singapore", 702, "sg", ["sg", "sgp"]),
    "CHN": ("China", 156, "cn", ["peoples republic of china", "cn", "chn"]),
    "USA": ("United States", 842, "us", ["usa", "u.s.a.", "united states of america", "us"]),
    "GBR": ("United Kingdom", 826, "uk", ["uk", "u.k.", "great britain", "england", "gb", "gbr"]),
    "DEU": ("Germany", 276, "de", ["deutschland", "de", "deu"]),
    "BEL": ("Belgium", 56, "be", ["be", "bel"]),
    "ESP": ("Spain", 724, "es", ["es", "esp"]),
    "ITA": ("Italy", 380, "it", ["it", "ita"]),
    "FRA": ("France", 251, "fr", ["fr", "fra"]),
    "RUS": ("Russia", 643, "ru", ["russian federation", "ru", "rus"]),
    "ARE": ("United Arab Emirates", 784, "ae", ["uae", "u.a.e.", "ae", "are"]),
    "SAU": ("Saudi Arabia", 682, "sa", ["ksa", "sa", "sau"]),
    "PAK": ("Pakistan", 586, "pk", ["pk", "pak"]),
    "BGD": ("Bangladesh", 50, "bd", ["bd", "bgd"]),
    "LKA": ("Sri Lanka", 144, "lk", ["lk", "lka"]),
    "JPN": ("Japan", 392, "jp", ["jp", "jpn"]),
    "KOR": ("South Korea", 410, "kr", ["korea", "republic of korea", "kr", "kor"]),
    "TUR": ("Turkey", 792, "tr", ["turkiye", "tr", "tur"]),
    "EGY": ("Egypt", 818, "eg", ["eg", "egy"]),
    "ZAF": ("South Africa", 710, "za", ["za", "zaf"]),
    "AUS": ("Australia", 36, "au", ["au", "aus"]),
    "CAN": ("Canada", 124, "ca", ["ca", "can"]),
    "MEX": ("Mexico", 484, "mx", ["mx", "mex"]),
    "BRA": ("Brazil", 76, "br", ["br", "bra"]),
}

_LOOKUP = {}
for _iso, (_name, _code, _tld, _aliases) in _COUNTRIES.items():
    for _k in [_iso, _name, *_aliases]:
        _LOOKUP[_k.strip().lower()] = _iso

CCTLD_TO_ISO3 = {v[2]: k for k, v in _COUNTRIES.items()}


def to_iso3(value):
    """Return ISO3 for a country name/alias/code, or '' when unknown."""
    if not value:
        return ""
    key = " ".join(str(value).strip().lower().split())
    return _LOOKUP.get(key, "")


def country_name(iso3):
    entry = _COUNTRIES.get(iso3)
    return entry[0] if entry else ""


def comtrade_code(iso3):
    entry = _COUNTRIES.get(iso3)
    return entry[1] if entry else None


def find_country_in_text(text):
    """ISO3 of the first known country *name* (not 2-letter code) found in text."""
    low = (text or "").lower()
    for iso, (name, _c, _t, aliases) in _COUNTRIES.items():
        for cand in [name.lower(), *[a for a in aliases if len(a) > 3]]:
            if cand in low:
                return iso
    return ""
