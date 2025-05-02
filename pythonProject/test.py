import requests
from bs4 import BeautifulSoup
import lxml

def request():
    url = f"https://m.place.naver.com/restaurant/1753946312/home"
    headers = {
        'Cookie': 'NAC=DBTFBYQb4hOc; NNB=KTI7UNZMHY3WO; ba.uuid=cadd4c0d-c562-45d1-87a2-2d3ed8af0ec8; _ga=GA1.2.130565033.1742265724; _ga_EFBDNNF91G=GS1.1.1742265724.1.0.1742265726.0.0.0; BNB_FINANCE_HOME_TOOLTIP_MYASSET=true; SRT30=1746011596; SRT5=1746012548; PLACE_LANGUAGE=ko; BUC=4ZixBB43r0UEExqFNsJeRq7m3XF4ITwqoJrUSDnsPvc=',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
    }

    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'lxml')
    print(soup.prettify())


if __name__ == "__main__":
    request()