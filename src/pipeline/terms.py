"""Deterministic text patterns shared by extraction and claim assessment.

Multilingual product words (Indonesian, Dutch) are included because the pilot markets use them.
These are heuristics: they find candidate sentences, they never create facts.
"""

import re

F = re.IGNORECASE
PRODUCT_RE = re.compile(r"\b(?:ground\s?nuts?|peanuts?|arachis(?:\s+hypogaea)?|kacang\s+tanah|đậu\s+phộng|pinda'?s?)\b", F)
# prepared / roasted forms are outside the raw-groundnut scope (HS 200811)
PREPARED_RE = re.compile(r"\b(?:roasted|salted|coated|flavou?red|peanut\s+butter|pindakaas|"
                         r"peanut\s+oil|snacks?|kacang\s+goreng|kacang\s+sangrai|cooked|fried|candy|brittle)\b", F)
RAW_FORM_RE = re.compile(r"\b(?:raw|in[\s-]shell|shelled|kernels?|blanched|splits?|unroasted|"
                         r"bold|java|hpS|natural|not\s+roasted)\b", F)
IMPORT_RE = re.compile(r"\bimport(?:s|ed|er|ers|ing|ation)?\b|\bimpor(?:tir|tasi)\b|\bnhập\s+khẩu\b|\binvoer\b", F)
NEGATION_RE = re.compile(r"\b(?:do(?:es)?\s+not|don't|doesn't|never|no\s+longer|not\s+(?:an?\s+)?(?:importer|import))\b", F)
INDIA_RE = re.compile(r"\bIndia(?:n)?\b", F)
ORIGIN_VERB_RE = re.compile(r"\b(?:import|sourc|procure|purchas|buy|origin|supplied\s+by|from)\w*", F)
SELL_RE = re.compile(r"\b(?:sell|sells|selling|supply|supplies|supplier|supplying|offer|offers|provide|provides|"
                     r"distribut\w+|wholesal\w+|trader|trading|our\s+products?|products?)\b", F)
PROCESS_RE = re.compile(r"\b(?:process(?:es|ed|ing|or)?|shell(?:s|ed|ing)?|blanch\w*|sort(?:ing|ed)?|"
                        r"grad(?:e|ed|ing)|mill\w*|manufactur\w+|roast\w*|packing|packaging)\b", F)

ROLE_PATTERNS = {
    "importer": re.compile(r"\bimporters?\b|\bimport(?:ing)?\s+(?:company|business|of)\b|\bwe\s+import\b", F),
    "distributor": re.compile(r"\bdistribut(?:or|ors|ion)\b", F),
    "wholesaler": re.compile(r"\bwholesal(?:er|ers|e)\b", F),
    "trader": re.compile(r"\btrad(?:er|ers|ing\s+company)\b", F),
    "processor": re.compile(r"\bprocess(?:or|ors|ing)\b|\bshelling\b|\bblanching\b", F),
    "manufacturer": re.compile(r"\bmanufactur(?:er|ers|ing)\b|\bfactory\b", F),
    "exporter": re.compile(r"\bexporters?\b", F),
    "retailer": re.compile(r"\bretail(?:er|ers)?\b", F),
}
ROLE_PRIORITY = ["importer", "distributor", "wholesaler", "trader", "processor", "manufacturer",
                 "exporter", "retailer"]
BUYER_ROLES = {"importer", "distributor", "wholesaler", "trader"}

CATEGORY_PATTERNS = {
    "raw groundnuts/peanuts": PRODUCT_RE, "peanut butter": re.compile(r"peanut\s+butter|pindakaas", F),
    "roasted/prepared peanuts": re.compile(r"roasted\s+(?:ground|pea)nuts?|salted\s+peanuts?", F),
    "peanut oil": re.compile(r"peanut\s+oil|groundnut\s+oil", F),
    "cashew nuts": re.compile(r"\bcashews?\b", F), "almonds": re.compile(r"\balmonds?\b", F),
    "sesame seeds": re.compile(r"\bsesame\b", F), "spices": re.compile(r"\bspices?\b", F),
    "rice": re.compile(r"\brice\b", F), "pulses/beans": re.compile(r"\b(?:pulses|beans|lentils)\b", F),
    "edible oils": re.compile(r"\bedible\s+oils?\b|\bcooking\s+oil\b", F),
    "nuts (general)": re.compile(r"\bnuts\b|\bkacang\b", F),
}

SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


def sentences(text: str):
    """Yield (sentence, index) for each sentence; lines are kept as hard boundaries."""
    out = []
    for line in (text or "").split("\n"):
        line = line.strip()
        if not line:
            continue
        for s in SENTENCE_SPLIT.split(line):
            s = s.strip()
            if s:
                out.append(s)
    return out


def is_prepared_only(sentence: str) -> bool:
    """True when the product mention is about prepared/roasted goods and gives no raw-form cue."""
    return bool(PREPARED_RE.search(sentence)) and not RAW_FORM_RE.search(sentence)
