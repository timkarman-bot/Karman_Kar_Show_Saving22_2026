import qrcode

BASE_URL = "https://vote.karmankarshowsandevents.com/vote/"

for car_id in range(1, 301):
    url = f"{BASE_URL}{car_id}"
    img = qrcode.make(url)
    img.save(f"qr_{car_id}.png")
