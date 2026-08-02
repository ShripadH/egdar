import requests


def get_cusip_from_openfigi(ticker: str) -> str:
    """
    Uses OpenFIGI's free REST API to map a US Stock Ticker to its 9-character CUSIP.
    """
    url = "https://api.openfigi.com/v3/mapping"
    headers = {'Content-Type': 'application/json'}

    # Payload specifying US equity ticker search
    payload = [{
        "idType": "TICKER",
        "idValue": ticker.upper(),
        "exchCode": "US"
    }]

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data and 'data' in data[0]:
                for item in data[0]['data']:
                    # Extract the 9-character CUSIP
                    cusip = item.get('cusip')
                    if cusip:
                        return cusip
        print(f"⚠️ No CUSIP found for {ticker}")
        return None
    except Exception as e:
        print(f"❌ OpenFIGI API error: {e}")
        return None


# Test the lookup:
if __name__ == "__main__":
    for symbol in ["AAPL", "TSLA", "NVDA", "MSFT"]:
        cusip = get_cusip_from_openfigi(symbol)
        print(f"Ticker: {symbol:6s} ──► CUSIP: {cusip}")

