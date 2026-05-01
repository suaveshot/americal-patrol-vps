"""Quick retry wrapper to fetch GBP account/location IDs."""
import time
from account_fetcher import _authed_session, ACCOUNT_MGMT_URL, BUSI_INFO_URL

session = _authed_session()
for attempt in range(5):
    print(f"Attempt {attempt + 1}...")
    resp = session.get(f"{ACCOUNT_MGMT_URL}/accounts")
    if resp.status_code == 200:
        for acct in resp.json().get("accounts", []):
            name = acct["name"]
            print(f"  account_id: {name}")
            r2 = session.get(
                f"{BUSI_INFO_URL}/{name}/locations",
                params={"readMask": "name,title"},
            )
            if r2.status_code == 200:
                for loc in r2.json().get("locations", []):
                    print(f"  location_id: {loc['name']}")
            else:
                print(f"  Locations request: {r2.status_code}")
        break
    print(f"  Got {resp.status_code}, waiting 30s...")
    time.sleep(30)
else:
    print("Still rate limited after 5 attempts.")
