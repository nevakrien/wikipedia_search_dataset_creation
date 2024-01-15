import wikipediaapi
import json
import requests

user_agent="search ml paired dataset for reaserch academic purposes(nevo.krien@gmail.com)"

with open('secrets.json') as f:
    secrets=json.load(f)
headers = {
    "Authorization": f"Bearer {secrets['Access token']}"
}

# # Initialize Wikipedia API for English and Hebrew
wiki_en = wikipediaapi.Wikipedia(language='en',user_agent=user_agent,headers=headers)
wiki_he = wikipediaapi.Wikipedia(language='he',user_agent=user_agent,headers=headers)


def get_article_pairs_from_english(title):
    # Get English page
    page_en = wiki_en.page(title)

    if not page_en.exists():
        return None,None

    # Get corresponding Hebrew page
    page_he_title = page_en.langlinks.get('he')
    if page_he_title:
        page_he = wiki_he.page(page_he_title.title)
        if page_he.exists():
            return page_en, page_he
        else:
            return page_en,None
    else:
        return page_en,None

def get_article_pairs_from_hebrew(title):
    # Get English page
    page_he = wiki_he.page(title)

    if not page_he.exists():
        return None,None

    # Get corresponding Hebrew page
    page_en_title = page_he.langlinks.get('en')
    if page_en_title:
        page_en = wiki_en.page(page_en_title.title)
        if page_he.exists():
            return page_en, page_he
        else:
            return None,page_he
    else:
        return None,page_he

def get_top_pages(language, access_type='all-access', year=None, month=None, day=None):
    if not year or not month or not day:
        today = datetime.date.today()
        year, month, day = today.year, today.month, today.day

    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{language}.wikipedia.org/{access_type}/{year}/{month:02d}/{day:02d}"
    response = requests.get(url,headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def parse_to_dict(page):
    sections=[x.text for x in page.sections]
    return {'title':page.title,'summary':page.summary,'sections':sections,'text':page.text}

if __name__=="__main__":
    e,h=get_article_pairs_from_english("Art")#("Stuff")#
    print(f"english: {e}\nhebrew: {h}")

    e,h=get_article_pairs_from_hebrew("הקו_הירוק")
    print(f"english: {e}\nhebrew: {h}")

    #print(dir(e))#.text)
    print({k:type(v) for k,v in parse_to_dict(e).items()})
    print([type(x) for x in parse_to_dict(e)['sections']])
    # Example usage
    #top_pages = get_top_pages('he', year=2018, month=1, day=7)#year=2024, month=1, day=7)
    #print(top_pages)
