from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

geolocator = Nominatim(user_agent="my_geocoder")

def get_lat_lng_by_address(address, retries=3):
    for i in range(retries):
        try:
            location = geolocator.geocode(address)
            if location:
                return location.latitude, location.longitude
        except GeocoderTimedOut:
            time.sleep(1)
    return None, None

# 예시 사용
address = "경상남도 창원시 의창구 봉곡동 37-3"
lat, lng = get_lat_lng_by_address(address)
print("위도:", lat, "경도:", lng)