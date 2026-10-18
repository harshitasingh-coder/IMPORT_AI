import json

from src.config import PROFILE_PATH, ensure_data_dirs, has_comtrade_key


def main():
    ensure_data_dirs()

    with PROFILE_PATH.open("r", encoding="utf-8") as file:
        profile = json.load(file)

    print("AVYUKT setup is ready.")
    print("Product:", profile["product_name"])
    print("Exporter:", profile["exporting_country"])
    print(
        "Broad HS code:",
        profile["broad_hs_code_for_initial_market_scan"]
    )
    if has_comtrade_key():
        print("UN Comtrade API: key found, client not connected yet.")
    else:
        print("UN Comtrade API: not connected yet.")


if __name__ == "__main__":
    main()
