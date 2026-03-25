
def bbg_fetch(underlying: str, field: str):
    mock_data = {
        ("AAPL US", "PX_LAST"): 189.5,
        ("MSFT US", "PX_LAST"): 412.3,
        ("TSLA US", "PX_LAST"): 176.8,
    }
    return mock_data.get((underlying, field), None)