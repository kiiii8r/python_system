# 必要なライブラリのインポート
from bs4 import BeautifulSoup
import pandas as pd
import requests
import datetime as dt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# データフレームの作成に必要なリストの定義
name_list = []
image_list = []
image_url_list = []
address_list = []
station_walk_list = []
hebe_list = []
tsubo_list = []
kai_list = []
price_list = []
price2_list = []
shiki_list = []
detail_url_list = []
content_list = []
get_date_list = []

# 今日の日付
today = format(dt.datetime.today(),'%Y/%m/%d')

# 居抜き本舗の検索ページ（フリーワード：ゴールデン街）
url:str = 'https://www.inuki-honpo.jp/rent/?mode_disp=&s_cd=&j_set=&offset=&sort=n_sort+DESC%2Cbukken_ctime+DESC&limit=&s_sort=n_sort+DESC%2Cbukken_ctime+DESC&s_unit=25&s_inuki_type%5B%5D=1&disp_prf=1&s_prf_cd%5B%5D=13&disp_city=0&disp_line=1&line_cd%5B%5D=588&disp_st=1&station_cd%5B%5D=588-231&station_cd%5B%5D=588-6327&disp_town=0&s_walk=&s_tsubo_min=&s_tsubo_max=&s_floor=&s_kakaku_min=&s_kakaku_max=&s_hosyokin_min=&s_hosyokin_max=&s_building_kakaku_min=&s_building_kakaku_max=&s_freeword=%E3%82%B4%E3%83%BC%E3%83%AB%E3%83%87%E3%83%B3&result%5B0%5D.x=57&result%5B0%5D.y=18#search_list'
# url:str = 'https://www.inuki-honpo.jp/rent/?mode_disp=&s_cd=&j_set=&offset=&sort=n_sort+DESC%2Cbukken_ctime+DESC&limit=&s_sort=n_sort+DESC%2Cbukken_ctime+DESC&s_unit=25&s_inuki_type%5B%5D=1&disp_prf=1&disp_city=0&disp_line=0&disp_st=0&disp_town=0&s_walk=&s_tsubo_min=&s_tsubo_max=&s_floor=&s_kakaku_min=&s_kakaku_max=&s_hosyokin_min=&s_hosyokin_max=&s_building_kakaku_min=&s_building_kakaku_max=&s_freeword=%E6%96%B0%E5%AE%BF&result%5B0%5D.x=55&result%5B0%5D.y=14#search_list'
page = requests.get(url)
soup = BeautifulSoup(page.content, 'html.parser')

# 物件ごと
elems = soup.find_all('table', class_='bukkenListTable')

for index, elem in enumerate(elems):
    
    # 取得日
    get_date_list.append(today)

    # 物件名
    name_list.append(elem.find('p', class_='bukkenListCat').text)
    
    # 物件画像
    image_url = 'https://www.inuki-honpo.jp/' + elem.find('a', class_='screenshot').find('img')['src']
    image_url_list.append(image_url)
    image_list.append('=IMAGE("' + image_url + '")')
    
    # 簡易住所
    address_list.append(elem.find('td', class_='bukkenListAdd delPad').text)
    
    # 駅徒歩
    station:str = elem.find('td', class_='bukkenListSta delPad').text.replace('\r', '').replace('\n', '').replace('\t', '')
    time:str = elem.find('td', class_='bukkenListStaMin delPad').text.replace('\r', '').replace('\n', '').replace('\t', '')
    station_walk_list.append(station + '：' + time)

    # 平米、坪数、階数
    elems_space = elem.find('td', class_='bukkenListM2').find_all('li')
    hebe_list.append(elems_space[0].text)
    tsubo_list.append(elems_space[1].text)
    kai_list.append(elems_space[2].text)
    
    # 賃料
    price_list.append(elem.find('span', class_='bukkenListPrice').text)
    
    # 坪単価
    price2_list.append(elem.find('span', class_='bukkenListPrice2').text)
    
    # 保証金・敷金・造作価格
    shiki_list.append(elem.find('td', class_='bukkenListshiki').find('li').text)
    
    # 詳細URL
    detail_url_list.append('https://www.inuki-honpo.jp/' + elem.find('a')['href'])
    
    # 説明文
    content_list.append(elem.find('p', class_='prDesc').text)
    
# データフレーム作成
df = pd.DataFrame()

df['取得日'] = get_date_list
df['物件名'] = name_list
df['物件画像'] = image_list
df['物件画像URL'] = image_url_list
df['詳細リンク'] = detail_url_list
df['住所'] = address_list
df['沿線・駅'] = station_walk_list
df['平米'] = hebe_list
df['坪数'] = tsubo_list
df['階層'] = kai_list
df['賃料'] = price_list
df['坪単価'] = price2_list
df['保証金・敷金・造作価格'] = shiki_list
df['説明文'] = content_list

# スプレッドシート連携
# creds を使って Google Drive API と対話するためのクライアントを作成
scope =['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(r'./python-spread.json', scope)
client = gspread.authorize(creds)

# ワークシートを開く
spreadsheet = client.open("居抜き本舗ゴールデン街定期スクレイピング")
worksheet = spreadsheet.worksheet('取得リスト')

# ワークシートからすべてのレコードを取得
records = worksheet.get_all_records()

# スプレッドシートのデータフレーム作成
df_sp = pd.DataFrame(records)

# スプシのデータ統合
df_con = pd.concat([df,df_sp])

# スプレッドシートのデータ更新
set_with_dataframe(worksheet, df_con.reset_index(drop=True))

print('更新完了')