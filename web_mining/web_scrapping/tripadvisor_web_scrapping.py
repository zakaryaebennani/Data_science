# -*- coding: utf-8 -*-
"""
Created on Tue Mar  4 15:02:31 2025

@author: zakar
"""
import requests
from bs4 import BeautifulSoup
USERNAME = 'usr'
PASSWORD = 'psd'
PROXY_URL = f'http://{USERNAME}:{PASSWORD}@unblock.oxylabs.io:60000'
proxies = {'http': PROXY_URL, 'https': PROXY_URL}
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Referer': 'https://www.google.com',
        'Connection': 'keep-alive',
    }

def get_url(do):
    data_offset_var = '-oa'+str(do)
    if do == 0:
        data_offset_var = ''
    url = f"https://www.tripadvisor.in/RestaurantSearch-g45963{data_offset_var}-a_date.2023__2D__03__2D__05-a_people.2-a_time.20%3A00%3A00-a_zur.2023__5F__03__5F__05-Las_Vegas_Nevada.html#EATERY_LIST_CONTENTS"
    print("URL to be scraped: ","\n", url, "\n")
    return url

scraping_control_variables = {
    'data_offset_lower_limit' : 0,
    'data_offset_upper_limit' : 510,
    'page_num' : 0,
    'page_size' : 30
}

def get_soup_content(do):
    url = get_url(do)
    response = requests.get(url, headers=headers, proxies = proxies, verify = False)
    soup_content = BeautifulSoup(response.content, "html.parser")
    return soup_content


def get_card(rest_num, soup_content):
    card_tag = f"{rest_num}_list_item"
    print(f"Scraping item number: {card_tag}")
    card = soup_content.find("div",{"data-test":card_tag})
    return card


def scrape_title(card):
    title = card.find_all('div', class_ = 'RfBGI')
    scraped_title = None if len(title) < 1 else title[0].text
    return scraped_title


def get_reviews_link(card):
    review_link = card.find("a", class_="aWhIG _S", href=True)

    if review_link:
        base_url = "https://www.tripadvisor.in"
        return base_url + review_link["href"]
    
    return None


def get_reviews_page(url):
    response = requests.get(url, headers=headers, proxies = proxies, verify = False)
    
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to fetch page: {response.status_code}")
        return None


def extract_reviewer_profiles(review_page):
    soup = BeautifulSoup(review_page, "html.parser")

    profiles = []
    
    for review_card in soup.find_all("div", class_="_c", attrs={"data-automation": "reviewCard"}):

        profile_link = review_card.find("a", class_="BMQDV _F Gv wSSLS SwZTJ")
        
        if profile_link and profile_link.get("href"):
            full_link = "https://www.tripadvisor.in" + profile_link.get("href")
            profiles.append(full_link)

    return profiles


def get_nationality(profile_url):
    response = requests.get(profile_url, headers=headers, proxies = proxies, verify = False)
    if response.status_code != 200:
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    

    location_span = soup.find("span", class_="PacFI _R S4 H3 LXUOn default")
    
    return location_span.text.strip().split(",")[0] if location_span else "Not available"


def parse_tripadvisor(scraping_control_variables):
    locations_scraped = []
    data_offset_lower_limit = scraping_control_variables['data_offset_lower_limit']
    data_offset_upper_limit = scraping_control_variables['data_offset_upper_limit']
    page_num = scraping_control_variables['page_num']
    page_size = scraping_control_variables['page_size']

    data_offset_current = data_offset_lower_limit
    
    while data_offset_current < data_offset_upper_limit :
        
        print("Scraping Page Number: ", page_num)
        print("Scraping Data Offset: ", data_offset_current)
        page_start_offset = (page_num*page_size) + 1
        page_end_offset = (page_num*page_size) + page_size + 1
        soup_content = get_soup_content(data_offset_current)
        
        for rest_num in range(page_start_offset , page_end_offset):
            card = get_card(rest_num, soup_content)
            if card is None:
                break
            
            print(scrape_title(card))
            link = get_reviews_link(card)
            review_page = get_reviews_page(link)
            profiles_url = extract_reviewer_profiles(review_page)
            for profile_url in profiles_url:
                locations_scraped.append(get_nationality(profile_url))

        print("Scraping Completed for Page Number: ", page_num, "\n" )
        print("Data Offset: ", data_offset_current)
        page_num = page_num + 1
        data_offset_current = data_offset_current + 30
    return locations_scraped




locations_scraped = parse_tripadvisor(scraping_control_variables)


from geopy.geocoders import Nominatim

def get_country_from_city(city):
    geolocator = Nominatim(user_agent="geo_lookup")
    location = geolocator.geocode(city)
    if location and location.address:
        return location.address.split(",")[-1].strip()
    return "Unknown"

from collections import Counter

countries = list(map(lambda city: get_country_from_city(city), locations_scraped))

country_counts = Counter(countries)

countries_classment = sorted(
    [[country, count] for country, count in country_counts.items() if country != "Unknown"],
    key=lambda x: x[1],
    reverse= True
)

def output(locations_scraped):
    from tabulate import tabulate
    with open("classemnet.txt", "w", encoding = "utf-8") as out:
        out.write(" le classement des nationalités les plus actives dans la ville sélectionnée : \n")
        obj = tabulate(countries_classment, headers = ["country", "number of reviews", "classement"], tablefmt = "fancy_grid")
        out.write(obj)
        print("success")
output(countries_classment)