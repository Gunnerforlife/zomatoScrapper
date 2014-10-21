__author__ = 'persi52'

import requests
import bs4
import psycopg2
import sys


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

def get_review_num(value):
    index = value.rfind('-')
    num_str_raw = value[index+1:]
    num_str = num_str_raw.strip()
    num_raw = int(num_str)
    switch_dict ={1:1.0,2:1.5,3:2.0,4:2.5,5:3.0,6:3.5,7:4.0,8:4.5,9:5.0}
    num = switch_dict[num_raw]
    return num

def add_rest_db(link):
        response = requests.get(link)
        soup = bs4.BeautifulSoup(response.text)
        name = soup.find_all('span', {'itemprop':'name'})[0].text
        address_raw = soup.find_all('h2', {'itemprop': 'address'})[0].text
        address = address_raw.strip()
        cuisines = soup.find_all('a', {'itemprop':'servesCuisine'})
        num = link.rfind('/')
        uid = link[num+1:]
        print name, address, uid
        cuisine_str = ''
        for i in range(0, len(cuisines)):
            if i == len(cuisines)-1:
                print cuisines[i].text
                cuisine_str = cuisine_str + cuisines[i].text
            else:
                print cuisines[i].text
                cuisine_str = cuisine_str + cuisines[i].text + ','
        print cuisine_str
        votes_count_str = soup.find('span', {'itemprop':'ratingCount'}).text
        votes_count = int(votes_count_str)
        print votes_count
        votes_value_str_raw = soup.find('div', {'itemprop':'ratingValue'}).text
        votes_value_str = votes_value_str_raw.strip()
        votes_value = float(votes_value_str)
        print votes_value
        print type(votes_value)
        stats = soup.find_all('div', {'class':'res-main-stats-num'})
        review_num = int(stats[0].text)
        bookmark_num = int(stats[2].text)
        checkins_num = int(stats[3].text)
        print review_num, bookmark_num, checkins_num, type(review_num), type(bookmark_num), type(checkins_num)
        try:
            cur.execute("SELECT * FROM restaurants WHERE uid = %s", (uid,))
            if cur.fetchall() != []:
                print 'data already exists'
            else:
                print 'inserting data'
                cur.execute("INSERT INTO restaurants (name, address,uid,votes_cnt,rating,review_cnt,book_cnt,checkin_cnt,cuisines) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(name, address, uid, votes_count, votes_value, review_num, bookmark_num, checkins_num, cuisine_str))
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
    foodie_lvl_str = soup.find('span', {'class':'user-stats_rank'}).text
    print foodie_lvl_str
    foodie_lvl = int(foodie_lvl_str)
    review_cnt_str = soup.find('a', {'data-tab':'reviews'}).text
    ind_strt = review_cnt_str.find('(')
    ind_end = review_cnt_str.rfind(')')
    review_str =review_cnt_str[ind_strt+1:ind_end]
    print review_str
    review = int(review_str)
    follower_cnt_str = soup.find('a', {'data-tab':'network'}).text
    ind_strt = follower_cnt_str.find('(')
    ind_end = follower_cnt_str.rfind(')')
    follower_str =follower_cnt_str[ind_strt+1:ind_end]
    print follower_str
    follower = int(follower_str)
    print name, bio, location, uid

    try:
        cur.execute("SELECT * FROM users WHERE uid = %s", (uid,))
        if cur.fetchall() != []:
            print 'data already exists'
        else:
            print 'inserting data'
            cur.execute("INSERT INTO users (name, bio, uid, address, review, foodie_lvl, follower) VALUES (%s, %s, %s, %s,%s,%s,%s)", (name, bio, uid, location, review, foodie_lvl, follower))

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
    ratings = []
    response = requests.get(main_link)
    soup = bs4.BeautifulSoup(response.text)
    review = soup.find_all('div', {'id':'reviews-container'})[0]
    names = review.find_all('div', {'class':'snippet-user--primary'})
    num = main_link.rfind('/')
    rest_uid = main_link[num+1:]
    cur.execute("SELECT id FROM restaurants WHERE uid=%s", (rest_uid,))
    #if cur.fetchone() == None:
        #rest_id = 0
   # else:
       # rest_id = cur.fetchone()
    print 'dude'
    rest_id = cur.fetchone()[0]
    print rest_id
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
    for desc in descs:
        final_desc = desc.find_all('div', {'class':'rev-text hidden'})
        if final_desc == []:
            final_desc = desc.find_all('div', {'class':'rev-text'})
        i = i+1
        print i
        rating_raw = final_desc[0].findAll('div')[1]['class'][4]
        ratings.append(get_review_num(rating_raw))
        print rating_raw
        temp = final_desc[0].findAll(text=True, recursive=False)
        final = unicode.join(u'\n', map(unicode, temp))
        #print temp
        #print type(temp)
        print final
        print type(final)
        reviews.append(final)
    for i in range(0, len(user_uids)):
        try:
            cur.execute("SELECT * FROM reviews WHERE user_uid = %s AND rest_uid = %s", (user_uids[i], rest_uid,))
            if cur.fetchall() != []:
                print 'data already exists'
            else:

                print 'inserting data'
                print reviews[i]
                print "%s" %reviews[i]
                cur.execute("INSERT INTO reviews (user_uid, rest_uid, review_text, rating) VALUES (%s, %s,%s,%s)", (user_uids[i], rest_uid, reviews[i], ratings[i],))

        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
            print 'Error %s' % e
            sys.exit(1)

        finally:
            con.commit()



for place in get_places_link():
    rest_pages(place)

for link_obj in rest_links:
    for link in link_obj:
        add_rest_db(link)

for link_obj in rest_links:
    for link in link_obj:
        get_user_link(link)

for link in user_links:
    add_user_db(link)

for link_obj in rest_links:
    for link in link_obj:
        add_reviews_db(link)

#rest_pages('https://www.zomato.com/ahmedabad/gurukul-restaurants')
#print rest_links

#add_rest_db()


# print user_links
# for a in user_links:
#     add_user_db(a)

#add_user_db('https://www.zomato.com/eshan')
#add_rest_db('https://www.zomato.com/ahmedabad/global-desi-tadkaa-satellite')
#get_rest_link('https://www.zomato.com/ahmedabad/satellite-restaurants')
#get_user_link('https://www.zomato.com/ahmedabad/global-desi-tadkaa-satellite')
#get_user_link('https://www.zomato.com/ahmedabad/global-desi-tadkaa-satellite')



