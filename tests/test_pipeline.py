from src.pipeline.data_pipeline import map_category

def test_map_category_exact_match():
    assert map_category("CHOCOLATE") == "Confectionery"

def test_map_category_keyword_match():
    # "VITAMIN" is a keyword for Health
    assert map_category("DAILY VITAMINS") == "Health"
    # "WINE" is a keyword for Beverages
    assert map_category("RED WINE 750ML") == "Beverages"

def test_map_category_fallback():
    # Unknown items should safely fall to 'Other'
    assert map_category("WEIRD UNKNOWN OBJECT") == "Other"
    assert map_category(None) == "Other"

def test_map_category_edge_cases():
    # Should handle weird casing and surrounding whitespace
    assert map_category("   cHoCoLaTe  ") == "Confectionery"
    assert map_category("red wine 750ml") == "Beverages"
    
    # Empty strings should map to 'Other'
    assert map_category("   ") == "Other"