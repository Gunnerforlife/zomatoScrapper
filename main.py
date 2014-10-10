__author__ = 'persi52'

import requests
import bs4
import psycopg2
import sys
import unicodedata
import re

root_url = 'https://www.zomato.com/'
rest_links = []
user_links = []

try:
    con = psycopg2.connect(database='mydb', user='persi52')
    cur = con.cursor()
except psycopg2.DatabaseError, e:
    if con:
        con.rollback()
    print 'Error %s' % e
    sys.exit(1)


def get_places_link():
    response = requests.get(root_url)
    soup = bs4.BeautifulSoup(response.text)
    return  [a.attrs.get('href') for a in soup.select('.neighbourhoods-list__link ')]

#def get_rest_link_main():


def get_rest_link(link):
    print link
    response = requests.get(link)
    soup = bs4.BeautifulSoup(response.text)
    obj = [a.attrs.get('href') for a in soup.select('.result-title')]
    rest_links.append(obj)
    #print list
    return 0


def get_user(rest):
    response = requests.get(rest)
    soup = bs4.BeautifulSoup(response.text)
    obj = [a.text for a in soup.select('.snippet__link')]
    rest_links.append(obj)




def rest_pages(link):
    print link
    response = requests.get(link)
    soup = bs4.BeautifulSoup(response.text)
    obj = [div.text for div in soup.select('.alpha')]
    new_string = str(obj[0])
    print new_string
    num = new_string.rfind('f')
    new_num = new_string[num+2:]
    final = int(new_num)
    print 'this is final'
    print final
    for i in range(1, final+1):
        print i
        if i > 2:
            break
        else:
            new_link = link+'?page='+str(i)
            get_rest_link(new_link)
    return 0


def get_user_link(link):
    print link
    response = requests.get(link)
    soup = bs4.BeautifulSoup(response.text)
    obj = soup.findChildren('div', {'class':'snippet__name'})
    #print obj
    for every_entry in obj:
        link = every_entry.find('a', {'data-entity_type':None})
        if link is not None:
            final_link = link['href']
            print final_link
            user_links.append(final_link)
    #obj = [a.attrs.get('href') for a in soup.select('.snippet__link')]
    return 0


def add_rest_db():
    for temp1 in rest_links:
        for link in temp1:
            response = requests.get(link)
            soup = bs4.BeautifulSoup(response.text)
            name = soup.find_all('span', {'itemprop':'name'})[0].text
            address_raw = soup.find_all('h2', {'itemprop': 'address'})[0].text
            address = address_raw.strip()
            cuisines = soup.find_all('a', {'itemprop':'servesCuisine'})
            num = link.rfind('/')
            uid = link[num+1:]
            print name, address, uid
            for cuisine in cuisines:
                 print cuisine.text

            try:
                cur.execute("SELECT * FROM restaurants WHERE uid = %s", (uid,))
                if cur.fetchall() != []:
                    print 'data already exists'
                else:
                    print 'inserting data'
                    cur.execute("INSERT INTO restaurants (name, address,uid) VALUES (%s, %s, %s)", (name, address, uid,))

            except psycopg2.DatabaseError, e:
                if con:
                    con.rollback()
                print 'Error %s' % e
                sys.exit(1)
            finally:
                con.commit()


def add_user_db(link):
    response = requests.get(link)
    soup = bs4.BeautifulSoup(response.text)
    name = soup.find_all('a', {'href':link})[0].text
    bio_raw = soup.find_all('div', {'class':'profile-bio'})
    print name, bio_raw
    if bio_raw == []:
         bio = ''
    else:
         bio = bio_raw[0].text.strip()
    location_row = soup.find_all('div', {'class': 'usr-location'})
    if location_row == []:
        location = ''
    else:
        location = location_row[0].text

    num = link.rfind('/')
    uid = link[num+1:]
    print name, bio, location, uid

    try:
        cur.execute("SELECT * FROM users WHERE uid = %s", (uid,))
        if cur.fetchall() != []:
            print 'data already exists'
        else:
            print 'inserting data'
            cur.execute("INSERT INTO users (name, bio, uid, address) VALUES (%s, %s, %s, %s)", (name, bio, uid, location))

    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)

    finally:
        con.commit()

    return 0


def add_reviews_db(main_link):
    user_uids = []
    reviews = []
    response = requests.get(main_link)
    soup = bs4.BeautifulSoup(response.text)
    review = soup.find_all('div', {'id':'reviews-container'})[0]
    names = review.find_all('div', {'class':'snippet-user--primary'})
    num = main_link.rfind('/')
    rest_uid = main_link[num+1:]
    for name in names:
        head = name.find_all('div', {'class':'snippet__head'})
        for every in head:
            link = name.a['href']
            num = link.rfind('/')
            user_uid = link[num+1:]
            user_uids.append(user_uid)
            print user_uid
    descs = review.find_all('div', {'itemprop':'description'})
    i = 0
    nothing = []
    for desc in descs:
        final_desc = desc.find_all('div', {'class':'rev-text hidden'})
        if final_desc == []:
            final_desc = desc.find_all('div', {'class':'rev-text'})
        i = i+1
        print i
        temp = final_desc[0].findAll(text=True, recursive=False)
        final = unicode.join(u'\n', map(unicode, temp))
        #print temp
        #print type(temp)
        print final
        print type(final)
        # print (temp[1].get('text'))
        # new_temp = unicodedata.normalize('NFKD', temp).encode('ascii', 'ignore')
        # print type(new_temp)
        # review_raw = new_temp.strip()
        # print review_raw
        # review_01 = re.sub(r'[ ]{2,}', "", review_raw)
        # review_02 = re.sub(r"\r\n'", r"' '", review_01)
        reviews.append(final)
    for i in range(0, len(user_uids)):
        try:
            cur.execute("SELECT * FROM reviews WHERE user_uid = %s AND rest_uid = %s", (user_uids[i], rest_uid,))
            if cur.fetchall() != []:
                print 'data already exists'
            else:
                print 'inserting data'
                #print reviews[i]
                #print "%s" %reviews[i]
                cur.execute("INSERT INTO reviews (user_uid, rest_uid, review_text) VALUES (%s, %s,%s)", (user_uids[i], rest_uid, reviews[i],))

        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
            print 'Error %s' % e
            sys.exit(1)

        finally:
            con.commit()




add_reviews_db('https://www.zomato.com/ahmedabad/barbeque-nation-gurukul')
#rest_pages('https://www.zomato.com/ahmedabad/gurukul-restaurants')
#print rest_links

#add_rest_db()

# for link_obj in rest_links:
#     for link in link_obj:
#         get_user_link(link)
# print user_links
# for a in user_links:
#     add_user_db(a)

# add_rest_db()
#get_rest_link('https://www.zomato.com/ahmedabad/satellite-restaurants')
#get_user_link('https://www.zomato.com/ahmedabad/global-desi-tadkaa-satellite')
#get_user_link('https://www.zomato.com/ahmedabad/global-desi-tadkaa-satellite')



