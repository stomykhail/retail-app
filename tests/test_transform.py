import polars as pl
from datetime import date
from src.pipeline.transform import clean_raw_data, process_valid_data

def test_clean_raw_data():
    """Tests that the initial cleaning phase correctly parses strings, dates, and flags errors."""
    mock_df = pl.DataFrame({
        "WEEK": ['"1231015"', "1231016", None], # Target: 2023-10-15
        "PLN": ["1001", "PLN", None],           # Middle row simulates header pollution
        "ACTUAL": [10.5, 0.0, None],
        "PROMO,,": ['"Y",,', "N", None],        # Simulates messy column name and trailing commas
        "SEG": ["NoSegment-A", "B", None],
        "PRODUCT_CATEGORY_LABEL": ['"Candy"', "Snacks", None],
        "OPSTUDY_LABEL": [" Test ", "Test2", None],
        "PLN_LABEL": ["Item 1", "Item 2", None]
    }).lazy()
    
    cleaned = clean_raw_data(mock_df).collect()
    
    # 1. Test Column Rename & String Stripping
    assert "PROMO" in cleaned.columns
    assert "PROMO,," not in cleaned.columns
    
    # 2. Test Valid Row Data Parsing
    valid_row = cleaned.filter(pl.col("ERROR_REASON") == "Valid").row(0, named=True)
    assert valid_row["DATE"] == date(2023, 10, 15)
    assert valid_row["PROMO"] == "Y"
    assert valid_row["SEG"] == "A"
    assert valid_row["PRODUCT_CATEGORY_LABEL"] == "CANDY"
    assert valid_row["OPSTUDY_LABEL"] == "TEST"
    
    # 3. Test Quarantine Tagging (Header Pollution & Missing Keys)
    header_row = cleaned.filter(pl.col("ERROR_REASON") == "Header Pollution").row(0, named=True)
    assert header_row["ERROR_REASON"] == "Header Pollution"
    
    missing_row = cleaned.filter(pl.col("ERROR_REASON") == "Missing Critical Keys").row(0, named=True)
    assert missing_row["ERROR_REASON"] == "Missing Critical Keys"

def test_golden_record_deduplication():
    """Tests that the pipeline correctly applies the most recent category/name to older sales."""
    mock_df = pl.DataFrame({
        "WEEK": ["old", "new"],
        "DATE": [date(2023, 1, 1), date(2023, 12, 31)],
        "PLN": ["1001", "1001"], # Same SKU
        "PLN_LABEL": ["Old Name", "New Name"],
        "BU": ["BU1", "BU2"],
        "PRODUCT_CATEGORY_LABEL": ["Old Cat", "New Cat"],
        "OPSTUDY_LABEL": ["Old Op", "New Op"],
        "PROMO": ["N", "Y"], "SEG": ["A", "A"], "ACTUAL": [5.0, 10.0], "ERROR_REASON": ["Valid", "Valid"]
    })
    
    processed = process_valid_data(mock_df)
    
    # Both sales events should survive, but the metadata should be unified to the Newest attributes
    assert processed.height == 2
    assert processed.select("PLN_LABEL").unique().item() == "New Name"
    assert processed.select("PRODUCT_CATEGORY_LABEL").unique().item() == "New Cat"

def test_null_alias_standardization():
    """Tests that 'na', 'none', and empty strings are correctly unified to 'Unknown'."""
    mock_df = pl.DataFrame({
        "WEEK": ["1"], "DATE": [date(2023, 1, 1)], "PLN": ["1001"],
        "PLN_LABEL": ["na"], "BU": ["none"], "PRODUCT_CATEGORY_LABEL": ["UNKNOWN"],
        "OPSTUDY_LABEL": [""], "PROMO": ["N"], "SEG": ["A"], "ACTUAL": [5.0], "ERROR_REASON": ["Valid"]
    })
    
    processed = process_valid_data(mock_df)
    
    # All those bad strings should have been converted to the standard "Unknown" category
    assert processed.select("PLN_LABEL").item() == "Unknown"
    assert processed.select("BU").item() == "Unknown"

def test_opstudy_polish_mapping():
    """Tests that ugly internal OPSTUDY acronyms are translated to readable text."""
    mock_df = pl.DataFrame({
        "WEEK": ["1"], "DATE": [date(2023, 1, 1)], "PLN": ["1001"], "PLN_LABEL": ["Item"],
        "BU": ["BU1"], "PRODUCT_CATEGORY_LABEL": ["Cat"], "PROMO": ["N"], "SEG": ["A"],
        "ACTUAL": [5.0], "ERROR_REASON": ["Valid"], "OPSTUDY_LABEL": ["NVLTY/GUM/MINT"] # Ugly Label
    })
    processed = process_valid_data(mock_df)
    assert processed.select("OPSTUDY_LABEL").item() == "Gum & Mints"

def test_exact_duplicate_dropping():
    """Tests that exact duplicate rows are completely removed."""
    mock_df = pl.DataFrame({
        "WEEK": ["1", "1", "1"],
        "DATE": [date(2023, 1, 1), date(2023, 1, 1), date(2023, 1, 1)],
        "PLN": ["1001", "1001", "1001"],
        "PLN_LABEL": ["Item", "Item", "Item"],
        "BU": ["BU1", "BU1", "BU1"],
        "PRODUCT_CATEGORY_LABEL": ["Cat", "Cat", "Cat"],
        "OPSTUDY_LABEL": ["Op", "Op", "Op"],
        "PROMO": ["N", "N", "N"],
        "SEG": ["A", "A", "A"],
        "ACTUAL": [5.0, 5.0, 5.0],
        "ERROR_REASON": ["Valid", "Valid", "Valid"]
    })
    
    processed = process_valid_data(mock_df)
    
    # All 3 identical rows should be collapsed into exactly 1 row
    assert processed.height == 1

def test_missing_value_imputation():
    """Tests that nulls in critical categorical columns are safely imputed."""
    mock_df = pl.DataFrame(
        {
            "WEEK": ["1"], "DATE": [date(2023, 1, 1)], "PLN": ["1001"],
            "PLN_LABEL": [None], "BU": [None], "PRODUCT_CATEGORY_LABEL": [None],
            "OPSTUDY_LABEL": [None], "PROMO": [None], "SEG": [None], 
            "ACTUAL": [10.0], "ERROR_REASON": ["Valid"]
        },
        schema_overrides={
            "OPSTUDY_LABEL": pl.String, "PROMO": pl.String, "SEG": pl.String,
            "PLN_LABEL": pl.String, "BU": pl.String, "PRODUCT_CATEGORY_LABEL": pl.String
        }
    )
    
    processed = process_valid_data(mock_df)
    
    # PROMO should default to 'N', and other missing categoricals should be 'Unknown'
    assert processed.select("PROMO").item() == "N"
    assert processed.select("SEG").item() == "Unknown"
    assert processed.select("PLN_LABEL").item() == "Unknown"

def test_clean_raw_data_ghost_row():
    """Tests that completely empty rows are caught by quarantine and don't crash the parser."""
    mock_df = pl.DataFrame({
        "WEEK": [None], "PLN": [None], "ACTUAL": [None], "PROMO": [None],
        "SEG": [None], "PRODUCT_CATEGORY_LABEL": [None],
        "OPSTUDY_LABEL": [None], "PLN_LABEL": [None]
    }).lazy()
    cleaned = clean_raw_data(mock_df).collect()
    assert cleaned.select("ERROR_REASON").item() == "Missing Critical Keys"