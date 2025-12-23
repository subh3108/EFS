
# !pip install pygooglenews
# !pip install gnewsclient
# !pip install snscrape==0.6.2.20230320
# !pip install rake_nltk

import pandas as pd
import os
# from newsapi import NewsApiClient
import requests
from pygooglenews import GoogleNews
from gnewsclient import gnewsclient
import snscrape.modules.twitter as sntwitter
from datetime import datetime, date, timezone
import json
import numpy as np
import pandas as pd
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from string import punctuation
import re
import numpy as np
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from rake_nltk import Rake
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
start_date = []
end_date = []
sources = ["bbc-news", "the-telegraph", "the-guardian-uk", "cnn", "abc-news-au",
           "dailymail.co.uk", "metro.co.uk", "mirror.co.uk", "news.google.com"]
all_keywords = ['strike', 'holiday', 'lockdown',
            'inflation', 'grocery sales', 'carnival', 'festival', 'party', 'Walmart', "Tesco", "Sainsbury's", "supply chain", "flood", "wendys", "lidl"]

# all_keywords = ['tesco', 'holiday']

# all_keywords = ['autumn', 'bank']
keywords = ['Lidl','Waitrose','Tesco','Walmart','Sainsbury\'s', 'Aldi', 'Asda', 'Marks & Spencers', 'Morrison\'s']
events = ['autumn bank holiday']
all_events = ['autumn bank holiday']

final_prod_events = pd.DataFrame()
counter = 6000
gnews_client_topics = ['Top Stories',
                       'World',
                       'Nation',
                       'Business',
                       'Technology',
                       'Entertainment',
                       'Sports',
                       'Science',
                       'Health']
# branch_keyword_bu_num = {'Esher' : 1, 'Dorchester' : 2}
branch_keyword_bu_num = {
'Peterborough': 103,
'trinity square' : 833,
'Gillingham': 105,
'Dorking': 107,
'St Ives': 108,
'Brighton': 114,
# 'Brent Cross': 119,
# 'Dorchester': 120,
# 'Esher': 121,
# 'Hall Green': 122,
# 'Whetstone': 124,
# 'Coulsdon': 129,
# 'New Malden': 131,
# 'Allington Park': 137
# 'Bury St Edmunds': 140,
# 'Blaby': 141,
# 'Marlow': 146,
# 'Kingsthorpe': 148,
# 'East Sheen': 149,
# 'Four Oaks': 150,
# 'Westbury Park': 151,
# 'Leighton Buzzard': 154,
# 'Stourbridge': 155,
# 'Bromley': 158,
# 'Birch Hill': 159,
# 'Ramsgate': 160
# 'Huntingdon': 163,
# 'Marlborough': 164,
# 'Green Street Green': 165,
# 'St Albans': 166,
# 'Stevenage': 167,
# 'Havant': 171,
# 'John Barnes': 174,
# 'Hertford': 175,
# 'Beaconsfield': 177,
# 'Enfield': 179,
# 'Goldsworth Park': 181,
# 'Sevenoaks': 182,
# 'St Neots': 185,
# 'Ruislip': 197,
# 'Banstead': 202,
# 'Ringwood': 203,
'Welwyn Garden City': 204,
'Ely': 205,
'Thame': 206,
'Chichester': 208,
'Southend': 213,
'Henley': 214,
'Finchley': 215,
'Godalming': 216,
'Monmouth': 217,
'Cirencester': 220,
'Berkhamsted': 223,
'Putney': 225,
'Salisbury': 226,
'Billericay': 229,
'Horley': 233,
'Okehampton': 234,
'Waterlooville': 239,
'Biggin Hill': 240,
'Banstead': 324
# 'Horsham New': 580,
# 'Heathfield': 595,
# 'Cambridge': 651,
# 'Hailsham': 653,
# 'Hythe': 654,
# 'Paddock Wood': 655,
# 'Saltash': 656,
# 'Sidmouth': 657,
# 'Sudbury': 658,
# 'Thatcham': 659,
# 'Worcester Park': 661,
# 'Wymondham': 662,
# 'Cheltenham': 663,
# 'Belgravia': 665,
# 'Tonbridge': 667,
# 'Chandlers Ford': 668,
# 'Portishead': 669,
# 'Romsey': 671,
# 'Wandsworth': 673,
# 'Newmarket': 674,
# 'Sandbach': 680,
# 'Fulham': 681,
# 'Towcester': 682,
# 'Abergavenny': 683,
# 'Hitchin': 685,
# 'Swaffham': 686,
# 'Newport': 687,
# 'Barry': 688,
# 'Worthing': 689,
# 'Otley': 691,
# 'Farnham': 692,
# 'Dartford': 693,
# 'Sheffield': 695,
# 'Wolverhampton': 696,
# 'Willerby': 697,
# 'Lichfield': 699,
# 'Wilmslow': 711,
# 'Lewes': 727,
# 'East Grinstead': 741,
# 'Buxton': 748,
# 'St Katharine Docks': 753,
# 'West Ealing': 764,
# 'Hersham': 765,
# 'Bishop s Stortford': 101,
# 'Buckhurst Hill': 102,
# 'Epsom': 104,
# 'Longfield': 109,
# 'Crowborough': 110,
# 'Holloway Road': 112,
# 'Milton Keynes': 115,
# 'Dibden': 118,
# 'Burgess Hill': 123,
# 'Temple Fortune': 125,
# 'Saffron Walden': 135,
# 'Evington': 136,
# 'Witney': 142,
# 'Harrow Weald': 143,
# 'Gosport': 152,
# 'Wantage': 153,
# 'Daventry': 156,
# 'Weybridge': 157,
# 'Winton': 161,
# 'Andover': 168,
# 'Southsea': 170,
# 'Kings Road': 173,
# 'Cobham': 176,
# 'Caterham': 178,
# 'Woodley': 180,
# 'Harpenden': 183,
# 'Caversham': 184,
# 'Northwood': 186,
# 'Richmond': 188,
# 'West Byfleet': 189,
# 'Sunningdale': 190,
# 'Barnet': 191,
# 'Chesham': 192,
# 'Bath': 193,
# 'Maidenhead': 194,
# 'Kingston': 195,
# 'Fleet': 196,
# 'Yateley': 198,
# 'Horsham': 200,
# 'Tenterden': 201,
# 'Bloomsbury': 207,
# 'Petersfield': 209,
# 'Stroud': 210,
# 'Abingdon': 211,
# 'Beckenham': 212,
# 'South Harrow': 219,
# 'Wokingham': 221,
# 'Norwich': 222,
# 'Bromley South': 224,
# 'Newark': 227,
# 'Gloucester Road': 230,
# 'South Woodford': 231,
# 'Surbiton': 232,
# 'Staines': 235,
# 'Marylebone': 236,
# 'Great Malvern': 237,
# 'Twyford': 238,
# 'Byres Road': 308,
# 'Weston Super Mare': 309,
# 'Wellington': 315,
# 'Ashbourne': 316,
# 'Storrington': 317,
# 'Menai Bridge': 318,
# 'Melksham': 319,
# 'Colchester': 455,
# 'JL Foodhall Oxford Street': 456,
# 'Pontprennau': 457,
# 'Crewkerne': 458,
# 'Kenilworth': 460,
# 'Eldon Square': 461,
# 'Westfield London': 462,
# 'Winchester': 463,
# 'Alcester': 474,
# 'Bridport': 475,
# 'Caldicot': 476,
# 'Croydon': 477,
# 'Haslemere': 478,
# 'Headington': 479,
# 'Holsworthy': 480,
# 'Leigh On Sea': 481,
# 'Ponteland': 482,
# 'Saxmundham': 483,
# 'Stamford': 484,
# 'Torquay': 485,
# 'Upminster': 486,
# 'Lutterworth': 487,
# 'Clerkenwell': 492,
# 'JL Foodhall Bluewater': 493,
# 'Altrincham': 494,
# 'Frimley': 652,
# 'Twickenham': 660,
# 'Canary Wharf': 664,
# 'Mill Hill': 670,
# 'Droitwich': 672,
# 'Wallingford': 675,
# 'Newbury': 676,
# 'Sanderstead': 677,
# 'Kensington': 678,
# 'Harrogate': 684,
# 'Rushden': 690,
# 'Lincoln': 694,
# 'Rickmansworth': 698,
# 'Ashford': 705,
# 'Cheadle Hulme': 710,
# 'Balham': 719,
# 'Southampton New': 720,
# 'Ampthill': 722,
# 'Durham': 730,
# 'Barbican': 732,
# 'Formby': 749,
# 'Comely Bank': 750,
# 'Christchurch': 754,
# 'Bayswater': 756,
# 'Eastbourne': 757,
# 'Chiswick': 760,
# 'Morningside': 761,
# 'Parkstone': 766,
# 'Clapham Junction': 767,
# 'Edgware Road': 768,
# 'Buckingham': 769,
# 'Windsor New': 772,
# 'Islington': 780,
# 'Hexham': 782,
# 'Harborne': 796,
# 'Brackley': 797,
# 'Lymington New': 798,
# 'Sandhurst': 799,
#
# 'Clifton': 834,
# 'Crouch End': 835,
# 'Oxted': 838,
# 'Enfield CFC': 199,
# 'Greenford CFC': 259,
# 'Evesham': 303,
# 'York': 311,
# 'Poynton': 312,
# 'East Cowes': 313,
# 'Wimbledon': 314,
# 'Knutsford': 326,
# 'Newton Mearns': 327,
# 'Stratford City': 328,
# 'Alton': 329,
# 'St Saviour (Jersey)': 332,
# 'Rohais (Guernsey)': 333,
# 'St Helier (Jersey)': 334,
# 'Admiral Park (Guernsey)': 335,
# 'Red Houses (Jersey)': 336,
# 'MOUNTSORREL': 403,
# 'Gerrards Cross': 459,
# 'Sevenoaks': 464,
# 'Marlow': 465,
# 'Cardiff Queen Street': 501,
# 'Acton': 502,
# 'Swindon': 504,
# 'Littlehampton': 505,
# 'Uckfield': 506,
# 'Hereford': 507,
# 'Malmesbury': 511,
# 'Coulsdon DFC': 513,
# 'Bagshot': 514,
# 'Nailsea': 515,
# 'Parsons Green': 516,
# 'Egham': 519,
# 'Jesmond': 520,
# 'Enfield Chase': 521,
# 'Sutton Coldfield': 522,
# 'Chippenham': 523,
# 'West Hampstead': 524,
# 'Shrewsbury': 525,
# 'Tottenham Court Road': 526,
# 'Dorking': 527,
# 'Wimbledon Hill': 528,
# 'Hawkhurst': 529,
# 'Fulham Palace Road': 530,
# 'Canterbury': 533,
# 'Sceptre (Watford)': 534,
# 'Kensington Gardens': 535,
# 'Camden': 536,
# 'Addlestone': 542,
# 'Fitzroy Street': 552,
# 'Teignmouth': 554,
# 'Hornchurch': 555,
# 'Edenbridge': 556,
# 'Keynsham': 557,
# 'Spinningfields': 558,
# 'Cheam': 559,
# 'Alderley Edge': 560,
# 'Walton-on-Thames': 562,
# 'Locks Heath': 563,
# 'Burgh Heath': 567,
# 'Petts Wood': 568,
# 'Portman Square': 569,
# 'Burnt Common': 571,
# 'Walbrook': 573,
# 'Leeds': 574,
# 'Broxbourne': 575,
# 'Amersham': 578,
# 'Bayswater Temp': 579,
# 'Oxford Botley Road': 581,
# 'BASINGSTOKE': 582,
# 'Old Brompton Road': 583,
# 'Hazlemere': 584,
# 'Ealing': 586,
# 'West Kensington': 587,
# 'Palmers Green': 588,
# 'Guildford': 589,
# 'Kings Cross': 590,
# 'Wollaton': 591,
# 'Rustington': 596,
# 'BATTERSEA NINE ELMS': 598,
# 'UTTOXETER': 599,
# 'High Holborn': 601,
# 'Alderley Old': 602,
# 'Sherborne': 604,
# 'Hove': 605,
# 'Leek': 606,
# 'High Wycombe': 607,
# 'Hampton': 612,
# 'Pimlico': 614,
# 'Foregate Street': 615,
# 'Clapham Common': 616,
# 'Kings Cross Station': 619,
# 'Stirling': 620,
# 'North Walsham': 622,
# 'Aylesbury': 625,
# 'Milngavie': 630,
# 'Ipswich': 632,
# 'Manchester Piccadilly': 636,
# 'Highbury Corner': 637,
# 'Muswell Hill': 639,
# 'Knightsbridge': 641,
# 'Solihull': 642,
# 'Sidcup': 643,
# 'Notting Hill Gate': 644,
# 'Truro': 648,
# 'Worcester': 700,
# 'Warminster': 701,
# 'Exeter': 702,
# 'South Bank Tower': 703,
# 'Bracknell': 706,
# 'Stratford Upon Avon': 708,
# 'Walton-le-Dale': 721,
# 'Bedford': 725,
# 'Wootton': 726,
# 'Market Harborough': 728,
# 'Poundbury': 733,
# 'Cowbridge': 735,
# 'ROEHAMPTON': 736,
# 'Battersea': 737,
# 'Bagshot Road': 738,
# 'Tubs Hill': 739,
# 'Greenwich': 740,
# 'Colmore Row (Birmingham)': 742,
# 'Ipswich (Corn Exchange)': 743,
# 'Kings Hill': 744,
# 'Chipping Sodbury': 751,
# 'Oakgrove': 752,
# 'Dorking': 755,
# 'Oundle': 758,
# 'Northwich': 759,
# 'Helensburgh': 771,
# 'Monument': 773,
# 'Little Waitrose at John Lewis Watford': 781,
# 'Victoria Street': 783,
# 'Vauxhall': 789,
# 'Horley - Brighton Road': 802,
# 'Wimborne': 805,
# 'Headington - London Road': 806,
# 'Guildford Worplesdon Road': 808,
# 'Little Waitrose John Lewis Southampton': 815,
# 'East Putney': 820,
# 'Meanwood': 828,
# 'Chester': 842,
# 'Raynes Park': 846,
# 'Oadby': 847,
# 'Leatherhead': 859,
# 'Victoria Bressenden Place': 860,
# 'SKY (OSTERLEY)': 865,
# 'Faringdon': 871,
# 'Haywards Heath': 873,
# 'Banbury': 874,
# 'Finchley Central': 876,
# 'Bromsgrove': 877,
# 'Winchmore Hill': 878
}
# England = ['Avon', 'Bedfordshire', 'Berkshire', 'Buckinghamshire', 'Cambridgeshire', 'Cheshire', 'Cleveland',
#            'Cornwall', 'Cumbria', 'Derbyshire', 'Devon', 'Dorset', 'Durham', 'East-Sussex', 'Essex', 'Gloucestershire',
#            'Hampshire', 'Herefordshire', 'Hertfordshire', 'Isle-of-Wight', 'Kent', 'Lancashire', 'Leice stershire',
#            'Lincolnshire', 'London', 'Merseyside',
#            'Middlesex', 'Norfolk', 'Northamptonshire', 'Northumberland', 'North-Humberside', 'North-Yorkshire',
#            'Nottinghamshire', 'Oxfordshire', 'Rutland', 'Shropshire', 'Somerset', 'South-Humberside', 'South-Yorkshire',
#            'Staffordshire', 'Suffolk', 'Surrey', 'Tyne-and-Wear', 'Warwickshire', 'West-Midlands', 'West-Sussex',
#            'West-Yorkshire', 'Wiltshire', 'Worcestershire']
England = ['London']
Wales = ['Clwyd', 'Dyfed', 'Gwent', 'Gwynedd', 'Mid-Glamorgan',
         'Powys', 'South-Glamorgan', 'West-Glamorgan']
# Wales = ['South-Glamorgan']
Scotland = ['Aberdeenshire', 'Angus', 'Argyll', 'Ayrshire', 'Banffshire', 'Berwickshire', 'Bute', 'Caithness',
            'Clackmannanshire', 'Dumfriesshire', 'Dunbartonshire', 'East-Lothian', 'Fife', 'Inverness-shire',
            'Kincardineshire', 'Kinross-shire',
            'Kirkcudbrightshire', 'Lanarkshire', 'Midlothian', 'Moray', 'Nairnshire', 'Orkney', 'Peeblesshire',
            'Perthshire', 'Renfrewshire', 'Ross-shire', 'Roxburghshire', 'Selkirkshire', 'Shetland', 'Stirlingshire',
            'Sutherland', 'West Lothian', 'Wigtownshire']
NorthernIreland = ['Antrim', 'Armagh', 'Down',
                   'Fermanagh', 'Londonderry', 'Tyrone']
# branch_keyword = ['Abergavenny', 'Alderley Edge', "Eastbourne", "Edenbridge", "Pontprennau"]
# branch_keyword = ['Abingdon', 'Canary Wharf']
# all_branch_keyword = ['Yateley', 'Canary Wharf', 'Workingham', 'Firmley']
all_branch_keyword = list(branch_keyword_bu_num.keys())
branch_keyword = all_branch_keyword
# countries = [England, Wales, Scotland, NorthernIreland]
countries = [England]
final = []
# final_prod = pd.DataFrame()
status_val = []


# --- Add these imports near the top ---
import feedparser
from urllib.parse import quote_plus

# --- Replace your googleNewsByStreet() with this ---

def googleNewsByStreet():
    """
    RSS-based collector for the last 15 days.
    Populates global 'final' with non-empty DataFrames matching downstream schema.
    """
    global final

    data = pd.DataFrame()

    # 15-day cutoff from current time in UTC
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=1)

    # Base RSS endpoint for Google News (UK English); scoring=n tends to sort by date
    base_rss = "https://news.google.com/rss/search?q={query}&hl=en-GB&gl=GB&ceid=GB:en&scoring=n"

    for branch in branch_keyword:
        for keyword in keywords:
            # Ask for last 15 days directly in query
            # (Google News supports 'when:15d' style time hints in queries)
            query = f"{branch} {keyword} when:15d"
            rss_url = base_rss.format(query=quote_plus(query))

            try:
                feed = feedparser.parse(rss_url)
            except Exception as e:
                print(f"[WARN] RSS parse failed for {query}: {e}")
                continue

            rows = []
            for entry in getattr(feed, "entries", []):
                # Try to get a structured time; fallback to text date
                dt = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                    dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
                else:
                    # Last resort: parse string date if present, else skip
                    text_date = entry.get("published") or entry.get("updated") or ""
                    if text_date:
                        try:
                            # feedparser sometimes provides parsed versions; if not, try fromisoformat-like fallback
                            dt = datetime.fromisoformat(text_date)  # may fail if not ISO
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            dt = None

                # If we couldn't parse a reliable date, keep it but we'll filter later by None
                # Hard filter: only keep entries that are within the last 15 days if dt is known
                if dt is not None and dt < cutoff:
                    continue  # too old

                title = entry.get("title", "")
                link = entry.get("link", "")
                published_str = entry.get("published", "") or entry.get("updated", "")

                media = ""
                try:
                    media = entry.source.title  # not always present
                except Exception:
                    pass

                if not title:
                    continue

                rows.append({
                    "title": str(title),
                    "media": str(media),
                    "datetime": dt.isoformat() if dt is not None else str(published_str),
                    "link": str(link),
                    "keyword": str(keyword),
                    "branch": str(branch),
                    "bu_num": branch_keyword_bu_num.get(branch, None),
                })

            if rows:
                df = pd.DataFrame(rows)
                # Secondary safety: if any rows lack a parsed dt, filter by textual datetime when possible
                # but primarily we've filtered above using 'dt'. We'll still deduplicate here.
                df = df.drop_duplicates(subset=["title"], keep="first")
                if not df.empty:
                    data = pd.concat([data, df], ignore_index=True)

    if not data.empty:
        final.append(data)
    else:
        print("[INFO] Collector produced no rows for the last 15 days (check network/proxy or queries).")

def _removeNonAscii(s):

    return "".join(i for i in s if ord(i) < 128)
def clean_text(text):
    text = text.lower()
    text = re.sub(r"what's", "what is ", text)
    text = text.replace('(ap)', '')
    text = re.sub(r"\'s", " is ", text)
    text = re.sub(r"\'ve", " have ", text)
    text = re.sub(r"can't", "cannot ", text)
    text = re.sub(r"n't", " not ", text)
    text = re.sub(r"i'm", "i am ", text)
    text = re.sub(r"\'re", " are ", text)
    text = re.sub(r"\'d", " would ", text)
    text = re.sub(r"\'ll", " will ", text)
    text = re.sub(r'\W+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r"\\", "", text)
    text = re.sub(r"\'", "", text)
    text = re.sub(r"\"", "", text)
    text = re.sub('[^a-zA-Z ?!]+', '', text)
    text = _removeNonAscii(text)
    text = text.strip()
    return text
def remove_stopwords(word_tokens):
    filtered_sentence = []
    stop_words = stopwords.words('english')
    specific_words_list = ['char', 'u', 'hindustan', 'doj', 'washington']
    stop_words.extend(specific_words_list)
    for w in word_tokens:
        if w not in stop_words:
            filtered_sentence.append(w)
    return filtered_sentence
def lemmatize(x):
    lemmatizer = WordNetLemmatizer()
    return ' '.join([lemmatizer.lemmatize(word) for word in x])
tokenizer = RegexpTokenizer(r'\w+')
def tokenize(x):
    return tokenizer.tokenize(x)
nltk.download('all')
def sentiment_analysis(prod):
    prod['combined_text'] = prod['title'].map(str)

    # applying all of these functions to the our dataframe
    prod['combined_text'] = prod['combined_text'].map(clean_text)
    prod['tokens'] = prod['combined_text'].map(tokenize)
    prod['tokens'] = prod['tokens'].map(remove_stopwords)
    prod['lems'] = prod['tokens'].map(lemmatize)
    sia = SIA()
    results = []
    for line in prod['lems']:
        pol_score = sia.polarity_scores(line)
        pol_score['lems'] = line
        results.append(pol_score)
    headlines_polarity = pd.DataFrame.from_records(results)
    temp = []
    # for line in prod['branch']:
        # temp.append(line)
    # headlines_polarity['branch'] = temp
    headlines_polarity['label'] = 0
    headlines_polarity.loc[headlines_polarity['compound'] > 0.2, 'label'] = 1
    headlines_polarity.loc[headlines_polarity['compound'] < -0.2, 'label'] = -1
    headlines_polarity['word_count'] = headlines_polarity['lems'].apply(lambda x: len(str(x).split()))
    headlines_polarity.head()
    # gk = headlines_polarity.groupby(['branch', 'label'])
    # fk = headlines_polarity.groupby('branch')['compound'].mean()
    # fk = fk.to_frame()
    result = [prod, headlines_polarity]
    headlines_polarity = headlines_polarity.rename_axis(index=None)
    return pd.merge(prod, headlines_polarity, on=["lems"], how="left")
from datetime import date

def outsource_news():
    googleNewsByStreet()
    prod = pd.concat(final)
    prod = prod.drop_duplicates('title', keep='first')
    print(prod)
    status_val.append(30)

    final_prod = sentiment_analysis(prod)

    # mail_data(final_prod) & upload_data_complete(final_prod)

    final_prod = final_prod.replace(np.nan,'',regex=True)

    # forecast_keywords = ['sale', 'sport', 'beverage', 'retail', 'vendor', 'market', 'morrisons', 'tesco', 'coles', 'business', 'shopping', 'weather',
    #                      'parties', 'events', 'walmart']

    second_keywords = ['bank holiday', 'heatwave', 'inflation', 'street party', 'rainfall', 'snow', 'retail', 'beverage', 'tesco', 'walmart', 'morrisons', 'weather',
                       'brc', 'mothers day', 'new store launch', 'lidl', 'homebase', 'walmart', 'new tesco store', 'coles', 'supermarket', 'shoppers', 'store', 'grocery', 'strike', 'holiday'
                       'shops', 'markets','holiday', 'lockdown','grocery sales', 'carnival', 'festival', 'party', "sainsbury", "supply chain", "flood", "wendys",
                       'ocado', 'spencer', 'asda']

    remove_keywords = ['accident', 'incident', 'injury', 'political', 'police', 'death', 'traffic', 'lord', 'war', 'actor', 'movie', 'star', 'lord', 'sex', 'gay',
                       'fight', 'crash', 'life', 'plans', 'weapons', 'dating', 'radio', 'tv', 'guinness', 'husband', 'fashion', 'attack']

    store_keywords = ['opens', 'closes', 'closed', 'opened', 'open', 'close',
                      'shut', 'confining', 'unopen', 'opening',
                      'close down', 'closing', 'shut down', 'conclude', 'ending', 'shutdown', 'closedown',
                      'closure', 'temporary', 'extended', 'shutting', 'launch', 'shuts', 'closures']

    store_remove_keywords = ['ftse', 'pubs', 'pub', 'life', 'stocks', 'earnings', 'dining', 'restaurants', 'stock', 'rocket', 'fashion', 'restaurant',
                             'letter', 'bills', 'investment', 'childrenswear', 'blizzard', 'infamous', 'qualifying', 'sports', 'bar', 'cafe',
                             'technology', 'dental', 'boobs', 'school','plans', 'flixbus', 'allegations', 'pharmacy', 'attack', 'driver', 'fitness', 'students',
                             'charities']

    competitor_keywords = ['tesco', 'wendys', 'lidl', 'sainsburys', 'sainsbury', 'aldi', 'morrisons', 'spencer', 'asda', 'supermarket',
                            'co', 'ocado', 'sparks', 'b&m', 'iceland', 'waitrose']

    print(final_prod)

    for index, row in final_prod.iterrows():
      if (len(np.intersect1d(row['tokens'], store_keywords)) == 0):
        # if(len(np.intersect1d(row['tokens'], competitor_keywords)) == 0):
        final_prod.drop(index=index, axis=0, inplace=True)
      else:
        if(len(np.intersect1d(row['tokens'], competitor_keywords)) == 0):
          final_prod.drop(index=index, axis=0, inplace=True)

    for index, row in final_prod.iterrows():
      for value in row['tokens']:
        val = value.capitalize()
        try:
            final_prod.at[index,'bu_num'] = branch_keyword_bu_num[val]
            final_prod.at[index,'branch'] = val
        except:
            n = 0

    final_prod = final_prod.drop_duplicates('title', keep='first')
    final_prod = final_prod.drop_duplicates('lems', keep='first')
    final_prod = final_prod.drop_duplicates('tokens', keep='first')

    final_prod['title'] = final_prod['title'].astype(str)

    final_prod['competitor_evt_indchar'] = ['Yes' if(len(np.intersect1d(x,competitor_keywords)) > 0) else 'No' for x in final_prod['tokens']]

    counter_guid = int(date.today().strftime("%Y%m%d"))
    final_prod['efsevt_guid'] = [(counter_guid*1000)+i for i in range(len(final_prod))]

    print(final_prod.dtypes)
    print(final_prod_events.dtypes)

    foriegn_key = []

    for index, row in final_prod.iterrows():
      flag = False
      for index_event, row_event in final_prod_events.iterrows():
        if(row['keyword'] != '' ):
          if(row['keyword'] in row_event['NAME']):
            print(row['keyword'],row_event['NAME'])
            foriegn_key.append(row_event['GUID'])
            flag = True
      if(flag == False):
        foriegn_key.append(0)

    print(foriegn_key)

    final_prod['guid'] = [(counter_guid*2000)+i for i in range(len(final_prod))]
    final_prod['fixed_annual_ind'] = 'n'
    final_prod['perm_env_ind'] = 'n'
    final_prod['cancelled_ind'] = 'n'
    final_prod['create_user'] = ''
    final_prod['update_user'] = ''
    final_prod['perm_env_ind'] = 'n'
    final_prod['crt_timestamp'] = date.today()
    final_prod['upd_timestamp'] = date.today()


    final_prod.rename(columns = {'link':'source_of_event'}, inplace = True)
    final_prod[["datetime"]] = final_prod[["datetime"]].astype(str)
    final_prod.columns = final_prod.columns.str.upper()

    final_prod.to_csv('Events.csv', mode='a', index=False, header=False)
    return final_prod
# !pip install geopy
# !pip install pgeocode

from geopy.geocoders import Photon, GoogleV3, Nominatim
import pgeocode
from math import cos, asin, sqrt, pi

def distance(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return round(12742 * asin(sqrt(a)),2)


def place_distance(string1,string2):
    try:
      geolocator_addr = Nominatim(user_agent="efs")
      # place = "Lidl,Bath"
      # place_2 = "Waitrose,Bath"
      place = string1 + "," + string2
      place_2 = "Waitrose," + string2
      # location = geolocator.geocode(place)
      pin = geolocator_addr.geocode(place)
      pin_2 = geolocator_addr.geocode(place_2)
      # print(location)
      print(pin)
      print(pin_2)
      print(pin.raw['lat'],pin.raw['lon'],pin_2.raw['lat'],pin_2.raw['lon'])
    except:
      return 'N/A'

    return distance(float(pin.raw['lat']),float(pin.raw['lon']),float(pin_2.raw['lat']),float(pin_2.raw['lon']))
road = []
# !pip install requests lxml
# !pip install beautifulsoup4
# !pip install geopy
# !pip install pgeocode

import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import Photon, GoogleV3, Nominatim
import pgeocode
from math import cos, asin, sqrt, pi
import re
import math

def distance_infrastructure(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return round(12742 * asin(sqrt(a)),2)


def place_distance_infrastructure(string2, work):
    temp = []
    try:
      route_df = pd.read_csv("Branch_Lat_Lon.csv")
      geolocator_addr = Nominatim(user_agent="http")
      place_2 = string2 + ", London, UK"
      pin_2 = geolocator_addr.geocode(place_2)
      print(pin_2)
      print(pin_2.raw['lat'],pin_2.raw['lon'])
      actual = 999999
      if(pin_2 != 'None'):
        for index, row in route_df.iterrows():
          dif = distance_infrastructure(float(pin_2.raw['lat']),float(pin_2.raw['lon']),row["lat"],row["lon"])
          if( dif < 20):
            if(actual > dif):
              actual = dif
              branch_name = row["branch"]
        if(actual <= 1):
          print(actual)
          temp.append(branch_name)
          print(branch_name)
          print("Yes")
          road.append([ string2 + " " + work, branch_name, "", date.today(), branch_keyword_bu_num[branch_name], actual,""])
          return branch_name
        print(actual)
    except:
      n = 0
    return None


def infrastructure():
  url = "https://tfl.gov.uk/traffic/status/?Input=&lineIds=&dateTypeSelect=Future%20date&direction=&startDate="+date.today().strftime("%Y-%m-%d")+"T00%3A00%3A00&endDate="+date.today().strftime("%Y-%m-%d")+"T23%3A59%3A59&lat=51.50721740722656&lng=-0.12758620083332062&placeType=stoppoint&input=London%2C%20UK"
  # url = "https://tfl.gov.uk/traffic/status/?Input=England%2C%20UK&lineIds=&dateTypeSelect=Future%20date&direction=&startDate=2023-06-23T00%3A00%3A00&endDate=2023-06-23T23%3A59%3A59&lat=52.35551834106445&lng=-1.1743197441101074&placeType=placeextra&input=london%2C%20uk"
  # Send a GET request to the website
  resp = requests.get(url)
  soup = BeautifulSoup(resp.text, "lxml")
  ele = soup.select('div[class^=\"road-disruption\"]')
  # print(ele[0].text)
  street = []
  works = []
  for element in ele:
      h2_tags = element.select('h4')
      p_tags = element.select('p[class^=\"topmargin\"]')
      date_tags = element.select('p[class^=\"highlight dates\"]')
      print(date_tags[0].text.strip("\n\n").split("\n"))
      for h2_tag,p_tag in zip(h2_tags,p_tags):
          # print(h2_tag.text.strip().split(" "))
          if("Works" in p_tag.text):
            arr = h2_tag.text.strip().split(" ")
            word = ""
            for i in range(1,len(arr)-1):
              if('(' not in arr[i]):
                word = word + " " + arr[i]
            # word = word + " " +arr[-1]
            street.append(word)
            works.append(p_tag.text)

  street = list(set(street))
  print(len(street))
  # print(street)

  for i,j in zip(street,works):
    place_distance_infrastructure(i,j)

  return road


# place_distance_infrastructure("Lea Gate, Blackpool Road, Preston", "")
# !pip install pretty-html-table
# !pip install pyshorteners
# !pip install xlsxwriter

import smtplib, ssl
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from pretty_html_table import build_table
from pyshorteners import Shortener
from io import StringIO, BytesIO
from email.mime.application import MIMEApplication
from datetime import date, timedelta
import xlsxwriter
from cryptography.fernet import Fernet

def mail_data(final_prod, final_prod_old):
  port = 465  # For SSL
  context = ssl.create_default_context()
  mail_df = pd.DataFrame()

  mail_df["TITLE"] = final_prod["TITLE"]
  mail_df["BRANCH"] = final_prod["BRANCH"]
  mail_df["SOURCE"] = final_prod["MEDIA"]
  mail_df["DATETIME"] = final_prod["DATETIME"]
  mail_df["BRANCH_NUM"] = final_prod["BU_NUM"]

  distance_arr = []
  for index, row in final_prod.iterrows():
    if(row["KEYWORD"] and row["BRANCH"]):
        distance_arr.append(place_distance(row["KEYWORD"], row["BRANCH"]))
  mail_df["DISTANCE IN MILES"] = distance_arr

  urls = []
  for index,row in final_prod.iterrows():
      x = Shortener().tinyurl.short(row["SOURCE_OF_EVENT"])
      urls.append(x)

  mail_df["LINK"] = urls

  for index, row in mail_df.iterrows():
    if(row["DISTANCE IN MILES"] != 'N/A'):
      if(row["DISTANCE IN MILES"] > 25):
          mail_df.drop(index=index, axis=0, inplace=True)

  all_prod = pd.concat([mail_df, final_prod_old])
  road = infrastructure()
  print(road)
  road_df = pd.DataFrame(road, columns = ["TITLE", "BRANCH", "SOURCE", "DATETIME", "BRANCH_NUM", "DISTANCE IN MILES", "LINK"])
  road_df = road_df.drop_duplicates("BRANCH", keep="first")

  html_table = mail_df.to_html(index=False, classes='example-table')
  road_table = road_df.to_html(index=False, classes='example-table')

  text = f"Hello Alex and Tim,\n Herewith attaching the events captured for all the competitors (core event types) including all the branches from "+ (date.today() - timedelta(days = 1)).strftime("%d-%m-%Y") +" to " + date.today().strftime("%d-%m-%Y") + " which are auto-generated from the script.\n\n\nThanks And Regards,\nSubhash\n\n\n"

  print(mail_df)

  html_table = html_table.replace('<th>', '<th style="padding: 10px 90px 10px 90px;">', 1)
  road_table = road_table.replace('<th>', '<th style="padding: 10px 80px 10px 80px;">', 1)

  if(mail_df.empty):
    html_table

# HTML Styling
  html = f'''
<html>
<head>
    <style>
        table.example-table th{{
              padding: 10px;
              text-align: center;
              background-color: #FFFFFF;
              font-weight: bold;
              font-size: 14px;
              width: 400px;
          }}

          table.example-table th:first-child {{
            padding: 20px 100px 20px 100px; /* Set the desired width for the fourth column */
          }}

          table.example-table td {{
            padding: 5px;
            color: black;
            font-size: 12px;
            width: 400px;
            font-family: Century Gothic, sans-serif;
          }}

        /* Add custom styles here */
    </style>
</head>
<body>
      <pre>{text}</pre>
        {html_table}
        <br/>
        <br/>
        {road_table}
</body>
</html>
'''

  part1 = MIMEText(html, 'html')
  msg = MIMEMultipart("alternative")
  msg['Subject'] = "Automated Event Capturing Model"
  recipients = ['subhash.verma@johnlewis.co.uk']
  # , 'alex.nicola@waitrose.co.uk', 'amit.kumbhar@johnlewis.co.uk', 'tim.feinberg@waitrose.co.uk', 'abhishek.jambhale@johnlewis.co.uk'
  # recipients = ['mitali.patel@johnlewis.co.uk']
  msg['To'] = ", ".join(recipients)
  msg.attach(part1)

  file_name = date.today().strftime("%d-%m-%Y") + "_Events.xlsx"
  # output = io.BytesIO()
  textStream = BytesIO()
  writer = pd.ExcelWriter(textStream, engine='xlsxwriter')
  all_prod.to_excel(writer,sheet_name="Competitor Events",index=False)
  road_df.to_excel(writer,sheet_name="Road Closure Events",index=False)
  writer.close()
  textStream.seek(0)
  attachment = MIMEApplication(textStream.read(), name= file_name)
  attachment['Content-Disposition'] = 'attachment; filename="{}"'.format(file_name)
  msg.attach(attachment)

  with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
      dec = str(Fernet('egupkHT3QJHG1c5dcPGiWEZaWdH04_uhgyD-8lYNxWM=').decrypt(b'gAAAAABpHy2IRPVaNZJU3a2jDD68rGtj0jMYEvJyrWRJepy-wUXuHwKdmAzMTSDXAWkP4S8tUWCd6Q5egqHWKGFkMx18sIu6NUPerPx9TSkeFpCedLP3LAc='), 'UTF-8')
      print("IN")
      server.login("subhash.verma@johnlewis.co.uk", dec)
      server.sendmail("subhash.verma@johnlewis.co.uk", recipients, msg.as_string())

  return all_prod, road_df
import gspread
if __name__ == '__main__':
  final_prod = outsource_news()
  #gc = gspread.service_account(filename='service_account.json')
  #worksheet_old = gc.open("05-08-2023_Events").sheet1
  #worksheet_old_2 = gc.open("05-08-2023_Events").get_worksheet(1)
  #worksheet_old_2.clear()
  #final_prod = outsource_news()
  #url = "https://docs.google.com/spreadsheets/d/1N-ql7D4kV-qfHzSN8YCpQw1oZVF_hWUcZHajsC1qU4s/export?format=xlsx"
  #df = pd.read_excel(url1, sheet_name = "Sheet1")
  final_prod_old = pd.DataFrame()
  #print(final_prod)
  #print(final_prod_old)
  #print(df)
  all_prod, road_df = mail_data(final_prod, final_prod_old)
  #worksheet_old.clear()
  #set_with_dataframe(worksheet_old, all_prod)
  #set_with_dataframe(worksheet_old_2, road_df)
  print("mail sent")