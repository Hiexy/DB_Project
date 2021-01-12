import psycopg2

from psycopg2 import sql

import pandas



lis = ('business','store','floor')

business_types = ("clothing", "supermarket", "banking", "entertainment", "services", "restaurants", "furniture", "coffeshops")



def setup():

    conn = psycopg2.connect(

    host="localhost",

    database="city_mall",

    user="postgres",

    password="postgres")  # Connect to the DBMS (PostgreSQL)



    cur = conn.cursor()  # Create cursor to execute SQL



    commands = (

        """drop table if exists floor cascade;""",

        """create table floor(id serial primary key,foot_traffic float(15),name varchar(255) unique not null);""",

        """INSERT INTO FLOOR(Foot_Traffic, Name) VALUES (238.21,'P1');"""

        """INSERT INTO FLOOR(Foot_Traffic, Name) VALUES (892.67,'GF');"""

        """INSERT INTO FLOOR(Foot_Traffic, Name) VALUES (381.34,'F1');"""

        """INSERT INTO FLOOR(Foot_Traffic, Name) VALUES (345.91,'F2');"""

        """INSERT INTO FLOOR(Foot_Traffic, Name) VALUES (405.102,'F3');""",



        """drop table if exists business cascade;""",

        """create table business (id serial primary key,phone_number varchar(10),location varchar(255),name varchar(255) unique not null,business_type varchar(255)not null);""",



        """INSERT INTO Business(Phone_Number, Location, Name,business_type ) VALUES(0778882121, 'Sweileh','Majid Al Futtaim Group','supermarket'); """,

        """INSERT INTO Business(Phone_Number, Location, Name,business_type ) VALUES(0781234561, 'Downtown', 'ALSHARYA','coffeeshops');""",

        """INSERT INTO Business(Phone_Number, Location, Name,business_type ) VALUES(0771234561, 'Dahiyat Al Rasheed', 'Telenor','services');""",

        """INSERT INTO Business(Phone_Number, Location, Name,business_type ) VALUES(1317139872, 'Uptown', 'Inditex group',' clothes');""",

        """INSERT INTO Business(Phone_Number, Location, Name,business_type ) VALUES(2315612291, 'Abdoun', 'The Movie Masters cinema group','entertainment');""",

        """INSERT INTO Business(Phone_Number, Location, Name,business_type ) VALUES(0771233451, 'Abdoun', 'Abdul Hameed Shoman','banking');""",

        """INSERT INTO Business(Phone_Number, Location, Name,business_type ) VALUES(0771214442, 'Tla_Ali', 'TBC corpration','furniture');""",

        """INSERT INTO Business(Phone_Number, Location, Name,business_type ) VALUES(1317139872,'Uptown', 'Chris Kempczinski','furniture');""",



        """drop table if exists store cascade;""",

        """create table store (id serial primary key,location varchar(255), contract_start date, contract_end date, name varchar(255) unique, space float(15),rented boolean,visibility float(10), sales int , rental_fee float(15));""",



        """alter table store add column  floor_id int """,

        """alter table store add constraint fk foreign key (Floor_id) references floor (id);""",

        """alter table store add column  business_id int ;""",

        """alter table store add constraint fk_1 foreign key (Business_id) references business (id);""",



        """INSERT INTO store (location, contract_start,contract_end ,name,space,rented,visibility, sales,rental_fee,Floor_id,Business_id) VALUES('Zone 4', ' 2013-02-03 ' , '2020-12-30 ','starbucks',306,false,0.6475,322,306.90,2,2);""",

        """INSERT INTO store (location, contract_start,contract_end ,name,space,rented,visibility, sales,rental_fee,Floor_id,Business_id) VALUES('Zone 1', '2013-01-01', '2024-01-01','Midas',250,true, 0.5,500,25555.355 ,2,7);""",

        """INSERT INTO store (location, contract_start,contract_end ,name,space,rented,visibility, sales,rental_fee,Floor_id,Business_id) VALUES('Zone 2', '2012-04-02' , '2024-04-02','macdonald',450,true,0.4451,74,13354.58,5,8);""",

        """INSERT INTO store (location, contract_start,contract_end ,name,space,rented,visibility, sales,rental_fee,Floor_id,Business_id) VALUES('Zone 3', '2010-04-01 ',' 2024-04-01 ','Arab Bank',908,true,0.1185,299,49295.61 ,1,6);""",

        """INSERT INTO store (location, contract_start,contract_end ,name,space,rented,visibility, sales,rental_fee,Floor_id,Business_id) VALUES('Zone 1', '2014-09-01' , '2019-01-01','Zara',578,false,0.2023,127,13379.99 ,5,4);""",

        """INSERT INTO store (location, contract_start,contract_end ,name,space,rented,visibility, sales,rental_fee,Floor_id,Business_id) VALUES('Zone 2', '2013-08-15' , '2024-08-15 ','grand cinema',847,true,0.9062,258,178424.97,2,5);""",

        """INSERT INTO store (location, contract_start,contract_end ,name,space,rented,visibility, sales,rental_fee,Floor_id,Business_id) VALUES('Zone 3', Null , Null,Null,316,false,0.4882,Null,Null,2,Null);""",

        """INSERT INTO store (location, contract_start,contract_end ,name,space,rented,visibility, sales,rental_fee,Floor_id,Business_id) VALUES('Zone 1', '2006-08-15 ' , '2030-08-15','carrefour',146,true,0.4957,695,20566.41,2,1);"""

        ) # SQL to create tables and populate them with a data set

    

    for i in commands:

        cur.execute(i)



    conn.commit()



    conn.close() # Close the connection to the DBMS



def check_type(typ):

    if typ not in business_types:

        return "other"

    return typ



def insert(t_option):

    conn = psycopg2.connect(

    host="localhost",

    database="city_mall",

    user="postgres",

    password="postgres")  # Connect to the DBMS (PostgreSQL)



    cur = conn.cursor()  # Create cursor to execute SQL



    if t_option == 1:

        num = input('\t\t\tEnter Number: ')

        loc = input('\t\t\tEnter Location: ')

        name = input('\t\t\tEnter Name: ')

        typ = input('\t\t\tEnter Business Type: ')

        typ = check_type(typ.lower())

        try:

            query="INSERT INTO Business(Phone_Number, Location, Name,business_type ) VALUES(%s,%s,%s,%s)"

            val = (num,loc,name,typ)

            cur.execute(query, val)

            conn.commit()

        except:

            print("Insert query not executed")



    if t_option == 2:



        

        con_start = input('\t\t\tEnter contract start (YYYY-MM-DD): ')

        if con_start == '':

            con_start = None



        con_end = input('\t\t\tEnter contract end (YYYY-MM-DD): ')

        if con_end == '':

            con_end = None



        name = input('\t\t\tEnter name: ')

        if name == '':

            name = None

        

        space = float(input('\t\t\tEnter space: '))

        rented = input('\t\t\tEnter T if rented or F if not: ')

        if rented == 'T':

            rented = True

        elif rented == 'F':

            rented = False

        else:

            print("Wrong Input, Insert query failed")

            return

        

        vis = float(input('\t\t\tEnter visibility between 0 and 1: '))

        if vis >= 1 or vis <= 0:

            print("Wrong Input, Insert query failed")

            return



        sales = input('\t\t\tEnter sales: ')



        if sales == '':

            sales = None

        else:

            sales = float(sales)



        

        get(3)

        loc = input('\t\t\tEnter location: ') 

        fid = int(input('\t\t\tEnter ID of floor: '))

        



        get(1)

        bid = input('\t\t\tEnter ID of Business: ')



        if bid == '':

            bid = None

        else:

            bid = int(bid)



        query = 'SELECT FOOT_TRAFFIC FROM FLOOR WHERE ID=%s'

        cur.execute(query, (fid, ))

        ft = cur.fetchone()[0]

        if sales == None:

            fee = 25000

        else:

            fee = vis * space * (sales / ft) * 365

        try:

            query="INSERT INTO store (location, contract_start,contract_end ,name,space,rented,visibility, sales,rental_fee,Floor_id,Business_id) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            val = (loc, con_start, con_end, name, space, rented, vis, sales, fee, fid, bid)

            cur.execute(query, val)

            conn.commit()

        except Exception as err:

            print("Insert query not executed. ERROR: {err}")



    if t_option == 3:

        ft = input('\t\t\tEnter Foot Traffic: ')

        name = input('\t\t\tEnter Name: ')

        try:

            query="INSERT INTO FLOOR(Foot_Traffic, Name) VALUES(%s,%s)"

            val = (ft,name)

            cur.execute(query, val)

            conn.commit()

        except:

            print("Insert query not executed")



    conn.close() # Close the connection to the DBMS



def upfee(id):

    conn = psycopg2.connect(

    host="localhost",

    database="city_mall",

    user="postgres",

    password="postgres")  # Connect to the DBMS (PostgreSQL)



    cur = conn.cursor()  # Create cursor to execute SQL

    query = 'SELECT VISIBILITY, SPACE, SALES, FLOOR_ID FROM STORE WHERE ID = %s'

    cur.execute(query, (id, ))

    tup = cur.fetchone()

    query = 'SELECT FOOT_TRAFFIC FROM FLOOR WHERE ID = %s'

    cur.execute(query, (tup[3], ))

    ft = cur.fetchone()[0]

    if tup[2] == None:

        fee = 25000

    else:

        fee = tup[0] * tup[1] * (tup[2] / ft) * 365

    query = 'UPDATE STORE SET rental_fee = %s WHERE ID = %s'



    cur.execute(query, (fee, id, ))

    conn.commit()

    conn.close() # Close the connection to the DBMS



def delete(tb_option):

    conn = psycopg2.connect(

    host="localhost",

    database="city_mall",

    user="postgres",

    password="postgres")  # Connect to the DBMS (PostgreSQL)



    cur = conn.cursor()  # Create cursor to execute SQL



    if tb_option == 1:

        get(tb_option)

        print('\t\t\tPick business to delete')

        id = int(input('\t\t\tPick ID of business: '))

        try:

            cur.execute('DELETE FROM BUSINESS WHERE ID = %s;',(id,))

        except:

            print("Delete did not work")

            return



    elif tb_option == 2:

        get(tb_option)

        print('\t\t\tPick store to delete')

        id = int(input('\t\t\tPick ID of store: '))

        try:

            cur.execute('DELETE FROM STORE WHERE ID = %s;',(id,))

        except:

            print("Delete did not work")

            return



    elif tb_option == 3:

        get(tb_option)

        print('\t\t\tPick floor to delete')

        id = int(input('\t\t\tPick ID of floor: '))

        try:

            cur.execute('DELETE FROM FLOOR WHERE ID = %s;',(id,))

        except:

            print("Delete did not work")

            return

        

    conn.commit()

    conn.close() # Close the connection to the DBMS



def update(tb_option):

    conn = psycopg2.connect(

    host="localhost",

    database="city_mall",

    user="postgres",

    password="postgres")  # Connect to the DBMS (PostgreSQL)



    cur = conn.cursor()  # Create cursor to execute SQL

    get(tb_option)

    id = int(input(f'\n\tPick ID of {lis[tb_option-1]}: '))

    query = sql.SQL('SELECT * FROM {} WHERE ID = %s').format(sql.Identifier(lis[tb_option-1]))

    try:

        cur.execute(query, (id,))

    except:

        print('Invalid ID')

    print('\t', cur.fetchone()[1:],sep='')



    if tb_option == 1:

        print("\tphone_number, location, name, business_type")



    if tb_option == 2:

        print("""\tlocation, contract_start, contract_end, name, space,

            rented, visibility, sales, rental_fee, Floor_id, Business_id""")

    

    if tb_option == 3:        

        print("\tfoot_traffic, name")



    att = input("\tSelect attribute to update: ").lower()

    upval = input('\tEnter new value: ')



    if att == 'business_type':

        upval = check_type(upval)



    

    query = sql.SQL('UPDATE {} SET {} = %s WHERE ID = %s').format(sql.Identifier(lis[tb_option-1]), sql.Identifier(att))

    try:

        cur.execute(query, (upval, id, ))

        if att == 'space' or att == 'visibility' or att == 'sales':

            upfee(id)

        elif att == 'foot_traffic':

            query = 'SELECT S.ID FROM STORE S, FLOOR F WHERE F.ID=%s AND S.FLOOR_ID = F.ID'

            cur.execute(query, (id,))

            ls = cur.fetchall()

            for i in ls:

                upfee(i[0])

    except:

        print('Update query did not work')

    

    conn.commit()

    conn.close() # Close the connection to the DBMS



def custom_sql():

    conn = psycopg2.connect(

    host="localhost",

    database="city_mall",

    user="postgres",

    password="postgres")  # Connect to the DBMS (PostgreSQL)



    cur = conn.cursor()  # Create cursor to execute SQL



    query = input('\tEnter custom sql query: ')

    try:

        cur.execute(query)

    except Exception as err:

        print('Wrong sql query, error: ', err)

    try:

        ls = cur.fetchall()

        for row in ls:

            print('\t\t\t',row,sep='')

    except Exception as err:

        print('Fetch failed, error: ', err)

    

    conn.commit()

    conn.close() # Close the connection to the DBMS



def get(tb_option):

    conn = psycopg2.connect(

    host="localhost",

    database="city_mall",

    user="postgres",

    password="postgres")  # Connect to the DBMS (PostgreSQL)



    cur = conn.cursor()  # Create cursor to execute SQL



    query = sql.SQL('SELECT * FROM {} ORDER BY ID').format(sql.Identifier(lis[tb_option-1]))

    cur.execute(query)



    d = dict()



    ls = cur.fetchall()

    if tb_option == 1:

        d["ID"] = []

        d["Phone Number"] = []

        d["Location"] = []

        d["Name"] = []

        d["Business Type"] = []

        for i in ls:

            d["ID"].append(i[0])

            d["Phone Number"].append(i[1])

            d["Location"].append(i[2])

            d["Name"].append(i[3])

            d["Business Type"].append(i[4])





    if tb_option == 2:

        d["ID"] = []

        d["Location"] = []

        d["Contract Start"] = []

        d["Contract End"] = []

        d["Name"] = []

        d["Space"] = []

        d["Rented"] = []

        d["Visibility"] = []

        d["Sales"] = []

        d["Rental Fee"] = []

        d["Floor ID"] = []

        d["Business ID"] = []

        for i in ls:

            d["ID"].append(i[0])

            d["Location"].append(i[1])

            d["Contract Start"].append(i[2])

            d["Contract End"].append(i[3])

            d["Name"].append(i[4])

            d["Space"].append(i[5])

            d["Rented"].append(i[6])

            d["Visibility"].append(i[7])

            d["Sales"].append(i[8])

            d["Rental Fee"].append(i[9])

            d["Floor ID"].append(i[10])

            d["Business ID"].append(i[11])





    if tb_option == 3:

        d["ID"] = []

        d["Foot Traffic"] = []

        d["Name"] = []

        for i in ls:

            d["ID"].append(i[0])

            d["Foot Traffic"].append(i[1])

            d["Name"].append(i[2])



    table = pandas.DataFrame(d).fillna("NULL")



    table = table.to_string(index=False)

    tb = table.split('\n')

    for i in tb:

        print('\t\t\t', i,sep='')



    conn.close() # Close the connection to the DBMS



if __name__ == "__main__":

    setup()  # Call setup

    print("""

    Welcome to City Mall's database.

    Read the instructions to interact with the database

    """)

    while(True):

        try:

            q_option = int(input("""

            Please choose one of the following options:

            Enter 1 to get contents of a table

            Enter 2 to insert a row into a table

            Enter 3 to delete a row from a table

            Enter 4 to update information in a specific table

            Enter 5 to enter a custom sql query

            Enter 6 to exit the interface

            """))  # Input query

        except:

            print("Wrong format of input, try again.")



        if q_option == 6:

            exit()

        elif q_option == 5:

            custom_sql()



        else:

            try:

                tb_option = int(input("""

                Please choose one of the following options:

                Enter 1 to choose the Business table

                Enter 2 to choose the Store table

                Enter 3 to choose the Floor table

                """))  # Input table

            except:

                print("Wrong format of input, try again.")



            if  3 < tb_option < 1:

                print("Incorrect table number try again.")

                continue



            if q_option == 1:

                get(tb_option)

            elif q_option == 2:

                insert(tb_option)

            elif q_option == 3:

                delete(tb_option)

            elif q_option == 4:

                update(tb_option)

            else:

                print("Incorrect option, Try again")