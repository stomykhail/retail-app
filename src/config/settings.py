import os

# Base project directory (Two levels up from src/config/settings.py)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# --- PATH CONFIGURATIONS ---
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_PATH = os.path.join(DATA_DIR, "raw", "source_1150208_to_1171119_part.csv")

SILVER_FILE = os.path.join(DATA_DIR, "silver", "silver_sales.parquet")
QUARANTINE_FILE = os.path.join(DATA_DIR, "quarantine", "quarantine_sales.parquet")

GOLD_DIM_DATE = os.path.join(DATA_DIR, "gold", "dim_date.parquet")
GOLD_DIM_PRODUCT = os.path.join(DATA_DIR, "gold", "dim_product.parquet")
GOLD_DIM_SEGMENT = os.path.join(DATA_DIR, "gold", "dim_segment.parquet")
GOLD_FACT_SALES = os.path.join(DATA_DIR, "gold", "fact_sales.parquet")

# --- MAGIC STRINGS & MAPPINGS ---
EXACT_MATCHES = {
    "CHOCOLATE": "Confectionery", "CANDY SINGLES": "Confectionery", "CANDY (NON-CHOCOLATE)": "Confectionery",
    "KING CHOCOLATE": "Confectionery", "THTR BX CHOC": "Confectionery", "TAKE HOME": "Confectionery",
    "NON CHOC LYDN": "Confectionery", "STANDARD SUGAR": "Confectionery", "CHOC SPECIALT": "Confectionery",
    "NON CHOC NICE": "Confectionery", "KING SUGAR": "Confectionery", "STANDARD CHOCO": "Confectionery",
    "SINGLES": "Confectionery", "NON CHOC PEG": "Confectionery", "CHOC LAYDOWN": "Confectionery",
    "PREMIUM CANDY BOX&EVRYDA": "Confectionery", "NOVELT": "Confectionery", "SUGAR FREE/BFY": "Confectionery",
    "XL/GIANT": "Confectionery", "NON CHOC HTM": "Confectionery", "CHOC HTM POUC": "Confectionery", "CHOC NICE": "Confectionery",
    "BEVERAGES": "Beverages", "SINGLE SERVE": "Beverages", "MP STILL 12CT+": "Beverages", "MS STILL 1L+": "Beverages",
    "SS SPARKLING 0-1": "Beverages", "SS ENERGY 0": "Beverages", "COFFEE": "Beverages", "MULTI SERVE": "Beverages",
    "WATER": "Beverages", "MP STILL 2-12CT": "Beverages", "SOFT DRINK  MP 12CT": "Beverages", "TEA SINGLE SERVE": "Beverages",
    "SS STILL 0-1L": "Beverages", "TEA MULTI SERVE": "Beverages", "ELECTROLY": "Beverages", "MS 2L": "Beverages", "K-CUP": "Beverages",
    "COUGH & COLD": "Health", "VITAMINS & SUPPLEMENTS": "Health", "COLD/FLU RE": "Health",
    "IBUPROFEN": "Health", "ACETAMINOPHEN": "Health", "PAIN/FEVER": "Health", "BANDAGES": "Health", "ALLERGY": "Health", 
    "NASAL SPRAY": "Health", "PSE": "Health", "ANTISEPTIC/F": "Health", "THRT BX SUGR": "Health", "EVERYDAY PIL": "Health",
    "THROAT LOZ/": "Health", "TABLETS/CAPSU": "Health", "WOUND DRESSI": "Health", "TESTING STRIP": "Health", "ALCOHOL/PERO": "Health",
    "HOMEOPATHIC": "Health", "PILLS/TABLET": "Health", "TOPICALS": "Health", "SINUS ALLER": "Health", "ASPIRIN": "Health", "PPIS": "Health",
    "ANTIHISTAMINES": "Health", "WRAPS/PATCHES": "Health", "NAPROXEN": "Health", "ORAL HEAL": "Health", "DRY EYE": "Health",
    "LENS CLEAN/DIS": "Health", "ANTIBIOTICS": "Health", "MULTI": "Health", "VIT D": "Health", "COUGH/COLD": "Health", "HEADACHE": "Health",
    "MULTI-GENERAL": "Health", "VIT B": "Health", "HYDROCORTISO": "Health", "DIPHENHYDRAMINE": "Health", "REDNESS RELIEF": "Health",
    "H2 BLOCKERS": "Health", "CETIRIZINE": "Health", "LORATADINE": "Health", "LAX FIBER PO": "Health", "JOINT": "Health", 
    "PREGNANCY TES": "Health", "MELATONIN": "Health", "ANTI-DIARRHEA": "Health", "COMPLIANCE AIDS": "Health", "CALCIUM": "Health", "FISH OIL": "Health", "ANTI-FUNGAL": "Health",
    "COSMETICS": "Beauty", "BATH & BODY": "Beauty", "LIP BALMS": "Beauty", "WIPES": "Beauty", "BODY CARE": "Beauty", "DENTAL CA": "Beauty",
    "POLISH": "Beauty", "PERMANENT": "Beauty", "BDY WASH/SHO": "Beauty", "LIPSTICK": "Beauty", "MASCARA": "Beauty", "FOUNDATION": "Beauty",
    "PENCIL LINER": "Beauty", "BAR SOAP": "Beauty", "NONPILLAR MA": "Beauty", "MANUAL": "Beauty", "THIN MAXIS": "Beauty", "BASE": "Beauty",
    "WHITENING": "Beauty", "THERAPEU": "Beauty", "PANTY LINERS/": "Beauty", "CLEANSER/SC": "Beauty", "INV SOLID M": "Beauty", "POWDER": "Beauty",
    "MENS": "Beauty", "MAXI PADS": "Beauty", "BASE TOOT": "Beauty", "DISP MEN": "Beauty", "SHADOW": "Beauty", "BASE MEN": "Beauty",
    "BASE WOMEN": "Beauty", "HAND SOAP": "Beauty", "DISP WMN": "Beauty", "EYE LASHES": "Beauty", "COTTON SWABS": "Beauty", "NATURAL-INSP": "Beauty",
    "DENTURE": "Beauty", "LIP": "Beauty", "CLEANSER/SCRUB": "Beauty", "SENS/ENAM": "Beauty", "HAND SANITIZ": "Beauty", "HAIR CARE": "Beauty",
    "PADS": "Beauty", "LIQUID LINER": "Beauty", "TAMP PREMIUM": "Beauty", "LUBRICATED": "Beauty", "CONCEALER": "Beauty", "ART NAIL": "Beauty",
    "ETHNIC": "Beauty", "EYEBROW PENCIL": "Beauty", "NAIL FI": "Beauty", "LIP GLOSS": "Beauty", "POLISH REMOVE": "Beauty", "INV SOLID W": "Beauty",
    "MOISTURIZER": "Beauty", "BRUSHES": "Beauty", "FLOSSERS": "Beauty", "BRUSH": "Beauty", "FACE": "Beauty", "ACCESSORIES": "Beauty", "ACCESSORI": "Beauty",
    "PAPER GOODS": "Paper", "FACIAL TIS": "Paper", "PAPER TOWE": "Paper", "PAPER PLAT": "Paper", "TOILET TIS": "Paper", "PLASTIC CU": "Paper",
    "LAUNDRY SOAP": "Household", "DISH/LIQ/AUT": "Household", "CLOTH/SHEET": "Household", "FOOD STORA": "Household", "ALL PURPO": "Household", "FABRIC SOFTE": "Household", "HOUSEWARES": "Household",
    "ALKALINE": "Electronics", "SYNC/CHA": "Electronics", "BUTTON": "Electronics", "EARBUDS": "Electronics",
    "CEREAL": "Grocery", "SOUPS": "Grocery", "CASHEWS": "Grocery", "ALMONDS": "Grocery", "POTATO": "Grocery", "TRAIL MIXES": "Grocery", "SYRUP": "Grocery", "CANNED MEAT/SEAFO": "Grocery",
    "MAINSTREAM COOKIE BASIC": "Grocery", "CORE - FILL IN": "Grocery", "BAR": "Grocery", "CORE - IMMEDIATE": "Grocery", "BFY - FILL IN": "Grocery", "HEAT&SRV": "Grocery",
    "CORE CRACKER BASIC": "Grocery", "PISTACHIOS": "Grocery", "CORE - FIL": "Grocery", "COOKIE MID SIZE": "Grocery", "EGGS": "Grocery", "CONDIMENT": "Grocery",
    "PACKAGED FRUIT": "Grocery", "PACKAGED VEGETABL": "Grocery", "MEAL REPL BARS S": "Grocery", "DINNER TO PREPARE": "Grocery", "ROLLED FL": "Grocery", "CORE - IMM": "Grocery",
    "INSTANT": "Grocery", "BAKING": "Grocery", "SPREADS": "Grocery", "MIXED NUTS": "Grocery", "SNACKS": "Grocery", "CORE - FILL": "Grocery", "PEANUTS": "Grocery",
    "TAPE": "Office", "PENS": "Office", "OFFICE SUPP": "Office", "PRM MRKR/": "Office", "FOLDERS": "Office", "NOTEBOOK": "Office", "PENCILS": "Office", "INDEX CARDS": "Office", "GLUE": "Office", "MAILERS": "Office", "POSTER B": "Office",
    "DOG TREATS/R": "Pets", "CAT FOOD": "Pets", "DOG FOOD": "Pets", "DISPOSABLE DIA": "Baby", "BABY FOOD": "Baby", "BABY": "Baby", "KIDS": "Baby", "BOTTLES": "Baby",
    "EASTER": "Seasonal", "VALENTIN": "Seasonal", "ACTION FIGURES/AC": "Toys", "ENTERTNMNT/": "Toys", "NONRLOAD AMT": "Financial", "RESTAURANTS": "Financial",
    "LIGHTERS": "Tobacco", "WOMENS UNDERWEA": "Apparel"
}

KEYWORD_MAPPINGS = {
    "Beverages": ["WINE", "BEER", "LIQUOR", "DRINK", "JUICE", "WATER", "TEA", "COFFEE", "SODA", "BEVERAGE", "LIQUID", "ISOTONIC", "CABERNET", "CHARDONNAY", "MERLOT", "MOSCATO", "PINOT", "VODKA", "RUM", "WHISKEY", "ICEE", "SHOT", "TETRA", "REF BEVERA", "SMOOTIE", "MIXER", "AQUA", "FLUID", "NATIONAL SNGL", "MULTI SERVE", "750ML", "INSTNT BE", "DOMESTIC CE"],
    "Confectionery": ["CHOC", "CANDY", "GUM", "MINT", "SWEET", "SUGAR", "NOVELTY", "JELLY", "SMACKER", "TRUFFLE", "BREATH"],
    "Health": ["VIT", "VITAMIN", "PAIN", "COLD", "COUGH", "ALLERG", "HEMORRHOID", "ANTI", "MEDICINE", "PILL", "CAPSULE", "TABLET", "RELIEF", "THERAP", "TREATMENT", "MINERAL", "SUPPLEMENT", "MEDICAL", "RX", "DIGESTIVE", "LANCET", "VAPO", "INSOLE", "EPSOM", "PROBIOTIC", "CONTRACEPT", "DRUG", "SORE", "SICKNE", "THERMOMETER", "DOSE", "AID", "HEARING", "LICE", "ENEMA", "GLOVE", "OTC", "FUNGUS", "RELAXER", "LAXATIVE", "TEST", "METER", "SLEEP", "BLOOD", "PRESSURE", "DIURETIC", "ARTHRITIS", "ASTHMA", "URINARY", "OSTOMY", "DIABET", "FEXOFENADINE", "CORN/CALLUS", "LEVOCETIRIZINE", "COQ10", "PROSTATE", "SYST", "HEART", "ORAL ANAL", "MASK", "SUPPOSITORI", "EAR PLUG", "MULTI-50", "MULTI-CHLD", "IMMUNITY", "WART", "LOZE", "KNEE", "CONTACT", "EAR WAX", "PRENATA", "SHARP", "EAR DROP"],
    "Beauty": ["HAIR", "SKIN", "NAIL", "COSMETIC", "MAKEUP", "LIP", "FACE", "BATH", "WASH", "SOAP", "SHAMPOO", "LOTION", "CREAM", "BLUSH", "BEAUTY", "DEOD", "SPF", "SUN", "COTTON", "TAMP", "PAD", "LINER", "ROUND", "BALL", "MOIST", "LUB", "SPONGE", "TWEEZER", "DEPILATOR", "POUF", "CONDITIONER", "GEL", "MENS", "WOMENS", "LADIES", "BRONZER", "PENCIL", "EYE", "FRAGRANCE", "COLOGNE", "PERFUME", "BODY", "POWDER", "CLEANSER", "SERUM", "RAZOR", "SHAVE", "BEARD", "MANICURE", "PEDICURE", "WIPE", "DOUCHE", "FEM", "COMB", "NIPPER", "BOBBY", "IMPLEMENT", "APPLICATOR", "CLINICAL", "ACNE", "AEROSOL", "BRUSH", "COLOR R", "STYLING", "GROOM", "CART MEN", "CART WMN", "FLUSHABLE", "INTERDENT", "PROTECTION 15", "COCOA BU", "COS ACCES", "BTH FRESH", "JAWS/CLAWS", "ALCOHOL FREE", "OIL", "PRE/AFTER", "DESIGNER FRA", "ROLL-ON", "DRYSPRY", "ROTARY", "SWAB", "ELAST"],
    "Pets": ["PET", "DOG", "CAT", "LITTER", "RAWHIDE", "BIRD"],
    "Toys": ["TOY", "GAME", "DOLL", "PLUSH", "FIGURE", "PUZZLE", "VEHICLE", "CARD", "ARTS", "CRAFT", "KITE", "BALLOON", "COSTUME", "TRADING", "PLAYING", "BEANIE", "BOARD", "OUTDR", "KIDS", "CHILD", "CONSOLE", "ONLINE GAMI"],
    "Paper": ["PAPER", "TISSUE", "NAPKIN", "CUP", "PLATE", "WRAP", "FOIL", "BOWL", "ENVELOPE", "STYROFOAM"],
    "Household": ["CLEAN", "DETERGENT", "BLEACH", "TRASH", "POLISH", "SPONGE", "LAUNDRY", "DISH", "HOUSE", "HOME", "MOP", "BROOM", "DUST", "AIR", "FRESHENER", "CANDLE", "PEST", "INSECT", "ROACH", "ANT", "MOUSE", "TRAP", "WEED", "LAWN", "GARDEN", "FURNITURE", "BLANKET", "TOWEL", "PILLOW", "KITCHEN", "COOK", "APPLIANCE", "TOOL", "HARDWARE", "LIGHT", "BULB", "MOTHBALL", "MOSQUITO", "REPELLAN", "FLYING", "RODENT", "PESTICIDE", "DECOR", "DISPOSABLE PAN", "CLOTH/SHEET", "TOILET BO", "FLOOR CAR", "NOTION", "RTU", "STAIN", "RELIGIOU"],
    "Grocery": ["SNACK", "CRACKER", "COOKIE", "CHIP", "NUT", "FOOD", "MEAT", "CEREAL", "BAR", "GROCERY", "BREAD", "CAKE", "POPCORN", "PRETZEL", "SAUCE", "MIX", "FROSTING", "MEAL", "EGGS", "CONDIMENT", "SPICE", "YOGURT", "SEED", "CHEESE", "PIZZA", "FRUIT", "VEGETABLE", "BUTTER", "PASTRY", "DESSERT", "SUSHI", "SANDWICH", "MUFFIN", "DIP", "ICE CREAM", "SYRUP", "BREAKFAST", "ALMOND", "CASHEW", "PECAN", "WALNUT", "MACADAMIA", "CORN", "TOMATO", "GARLIC", "CINNAMON", "SOUP", "BAKING", "WHEAT", "CORE - IMMED", "CORE - FILL", "PRE-POPPED", "BETTER FO", "BANANA"],
    "Electronics": ["BATTERY", "ELECTRONIC", "CABLE", "PHONE", "SYNC", "EARBUD", "CHARGER", "USB", "SPEAKER", "HEADPHONE", "AUDIO", "VIDEO", "TV", "DVD", "CAMERA", "COMPUTER", "MEDIA", "GADGET", "HALOGEN", "LED", "ALKALINE", "WIRE", "CORD"],
    "Office": ["OFFICE", "SCHOOL", "PEN", "PENCIL", "MARKER", "BINDER", "FOLDER", "TAPE", "GLUE", "STICKER", "CALENDAR", "JOURNAL", "MEMO", "STATIONERY", "BOOK", "NOTE", "INDEX", "MAILER", "POSTER", "CALCULATOR", "COMPASS", "RULER", "DESK", "FILLER PAPE", "PUBLISH"],
    "Baby": ["BABY", "INFANT", "TODDLER", "DIAPER", "PACIFIER", "BOTTLE", "TEETHING", "SWIM PANT"],
    "Apparel": ["APPAREL", "SHIRT", "PANT", "SHOE", "SOCK", "HOSIERY", "HAT", "GLOVE", "SCARF", "CLOTHING", "DRESS", "LEGGING", "TIGHT", "CASUAL", "ATHLETIC", "UNDERWEAR", "BRA", "SLIPPER", "BOOT", "SANDAL", "UMBRELLA", "BAGS", "TOTE", "BACKPACK", "WALLET", "JEWELRY", "WATCH", "SUNGLASS", "ACCESSOR", "UMBREL", "SHEER"],
    "Seasonal": ["EASTER", "VALENTINE", "CHRISTMAS", "HALLOWEEN", "HOLIDAY", "SEASONAL", "SUMMER", "SPRING", "XMAS", "PATRIOTIC", "MOTHERS DA", "FATHERS DA", "WINTER", "AUTUMN", "CHRISTMA"],
    "Financial": ["GIFT CARD", "FINANCIAL", "PREPAID", "DEBIT", "RESTAURANTS"],
    "Tobacco": ["TOBACCO", "CIGAR", "CIGARETTE", "LIGHTER", "VAPOR", "SMOKING", "HOOKAH", "SHISHA", "E-CIG", "VAPE"],
    "Other": ["ALL OTHER", "MISC", "UNKNOWN", "CONTINU", "VALUE", "ASSORTED", "BASIC", "SPECIALTY", "PREMIUM", "GENERAL RET", "B2G1", "PROMO", "PROGRAM", "ONE TIME", "DEPARTMENT", "FEATURE", "TRAY VAL", "WEB EXCL", "POUCH", "BOX", "INS", "REGULAR", "LARGE", "MEDIUM", "SMALL", "MULTI PACK", "DEVICES", "PLUS", "UNISEX", "TEMPORARY", "JAR", "BAG", "POW"]
}

OPSTUDY_POLISH = {
    "NVLTY/GUM/MINT": "Gum & Mints",
    "RTD/TEA/COFFEE": "Ready-to-Drink Beverages",
    "HHD SP/DET": "Household Detergents",
    "DENTAL NDS": "Dental Needs",
    "COSM-EYE": "Eye Cosmetics",
    "COSM-FACE": "Face Cosmetics",
    "GENERAL GROCERIES": "General Groceries",
    "TRAVEL/TRIAL": "Travel Size Items"
}