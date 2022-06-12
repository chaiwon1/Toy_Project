from re import I
import requests
import json
import sqlite3

#OpenAPI로 데이터 요청
url = 'http://apis.data.go.kr/B552584/UlfptcaAlarmInqireSvc/getUlfptcaAlarmInfo'
params ={'serviceKey' : '서비스키', 'returnType' : 'json', 'numOfRows' : '1000', 'year' : '2020', 'itemCode' : 'PM10' }

#json 파일로 저장 + 필요한 부분만 추출
response = requests.get(url, params=params)
parsed_data = json.loads(response.text)
data1 = parsed_data['response']['body']['items']

#빈 list 만들어주고 필요한 데이터만 저장
data_list = []
for i in range(len(data1)) :
    data = data1[i]
    data.pop('sn')
    data_list.append(data)

#connection, cursor 생성    
conn = sqlite3.connect('public_data.db')
cur = conn.cursor()

#table 있으면 삭제
cur.execute("DROP TABLE IF EXISTS public_table")

#table 생성
cur.execute("""CREATE TABLE public_table(
                                            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                            clearVal INTEGER,
                                            districtName VARCHAR,
                                            dataDate DATE,
                                            issuVal INTEGER,
                                            issuTime TIME,
                                            clearDate DATE,
                                            issuDate DATE, 
                                            moveName VARCHAR,
                                            clearTime TIME,
                                            issueGbn VARCHAR,
                                            itemCode VARCHAR
                                        )
            """)

#table에 데이터 저장
for i in data_list :
    temp = list(i.values())
    temp_list = []
    temp_list.append(temp)

    for j in temp_list :
        cur.execute("""INSERT INTO public_table(clearVal, districtName, dataDate, issuVal, 
                                            issuTime, clearDate, issuDate, moveName, 
                                            clearTime, issueGbn, itemCode) 
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",  j)

conn.commit()