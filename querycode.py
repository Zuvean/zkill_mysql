# Python Object-Oriented Programming
import requests
import pprint
import pymysql
import copy
import time
import mysql.connector

class KillMail:

    def __init__(self, original, package):
        self.original = original
        self.package = package

def retrieve():
    
    response = requests.get("https://redisq.zkillboard.com/listen.php?queueID=DataGod9674429039")
    data = response.json()
    data_killmail = data['package']

    if data_killmail == None:
        time.sleep(10)
        print("null object")
    else:
        print("Killmail Found")
        return data_killmail

def sql_update(query, data):
    cnx = mysql.connector.connect(user='admin', database='Zkill', host='database-2.cywxzrwutyd7.us-west-1.rds.amazonaws.com', password='harlan1j')
    cursor = cnx.cursor()
    cursor.executemany(add_employee, data_employee)
    cnx.committ()
    cursor.close()
    cnx.close()

def bit_convert(string):
    if string == 'false':
        return '0'
    
    elif string == 'true':
        return '1'
    
    else:
        #print("Not a boolean")
        pass

def sql_connect():
    data = copy.deepcopy(retrieve())
    print("pre-connection")
    cnx = mysql.connector.connect(user='admin', password='harlan1j',
                              host='database-3.cywxzrwutyd7.us-west-1.rds.amazonaws.com',
                              database='Zkill')
    print("connection working")
    cursor = cnx.cursor()
    print("connection successful")    
    
    killmail_ID = data['killID']
    data_killmail = data['killmail']

    frame_insert_query = ("INSERT INTO Frame "
                        "(killID, SolarSystemID, DateTime, MoonID, WarID) "
                        "VALUES (%(killID)s, %(SolarSystemID)s, %(DateandTime)s, %(MoonID)s, %(WarID)s)")

    date_time_start = copy.deepcopy(data_killmail['killmail_time'])
    date_time_1 = date_time_start.replace('Z', '')
    date_time_finish = " ".join(date_time_1.split(sep='T',maxsplit=1))
    
    frame_data = {
        'killID' : killmail_ID,
        'SolarSystemID' : data_killmail['solar_system_id'],
        'DateandTime' : date_time_finish,
        'MoonID' : data_killmail.get('moon_id'),
        'WarID' : data_killmail.get('war_id'),
    }

    cursor.execute(frame_insert_query, frame_data)

    attackers_insert_query = ("INSERT INTO Attackers "
                            "(killID, AllianceID, CorporationID, CharacterID, damage_done, FactionID, final_blow, security_status, ShipTypeID, WeaponTypeID) "
                            "VALUES (%(killID)s, %(AllianceID)s, %(CorporationID)s, %(CharacterID)s, %(damage_done)s, %(FactionID)s, %(final_blow)s, %(security_status)s, %(ShipTypeID)s, %(WeaponTypeID)s)")

    attackers_data =  data_killmail['attackers']
    attackers_lst = []

    for attackers in attackers_data:
        attackers_temp = {
            'killID' : killmail_ID,
            'AllianceID' : attackers.get('alliance_id'),
            'CorporationID' : attackers.get('corporation_id'),
            'CharacterID' : attackers.get('character_id'),
            'damage_done' : attackers.get('damage_done'),
            'FactionID' : attackers.get('faction_id'),
            'final_blow' : attackers.get('final_blow'),
            'security_status' : attackers.get('security_status'),
            'ShipTypeID' : attackers.get('ship_type_id'),
            'WeaponTypeID' : attackers.get('weapon_type_id'),
        }
        attackers_lst.append(copy.deepcopy(attackers_temp))
    
    cursor.executemany(attackers_insert_query, attackers_lst)

    items_insert_query = ("INSERT INTO Items "
                        "(killID, flag, ItemTypeID, QuantityDropped, QuantityDestroyed, Singleton) "
                        "VALUES (%(killID)s, %(flag)s, %(ItemTypeID)s, %(QuantityDropped)s, %(QuantityDestroyed)s, %(Singleton)s)")
    
    items_data = data_killmail['victim']['items']
    items_lst = []

    for item in items_data:
        items_temp = {
            'killID' : killmail_ID,
            'flag' : item.get('flag'),
            'ItemTypeID': item.get('item_type_id'),
            'QuantityDropped': item.get('quantity_dropped'),
            'QuantityDestroyed': item.get('quantity_destroyed'),
            'Singleton': item.get('singleton'),
        }
        items_lst.append(copy.deepcopy(items_temp))

    cursor.executemany(items_insert_query, items_lst)

    victims_insert_query = ("INSERT INTO Victims "
                        "(killID, AllianceID, CorporationID, CharacterID, damage_taken, xPos, yPos, zPos, ShipTypeID) "
                        "VALUES (%(killID)s, %(AllianceID)s, %(CorporationID)s, %(CharacterID)s, %(damage_taken)s, %(xPos)s, %(yPos)s, %(zPos)s, %(ShipTypeID)s)")
    
    victim_killmail = copy.deepcopy(data_killmail['victim'])
    victims_data = {
        'killID' : killmail_ID,
        'AllianceID' : victim_killmail.get('alliance_id'),
        'CorporationID' : victim_killmail.get('corporation_id'),
        'CharacterID' : victim_killmail.get('character_id'),
        'damage_taken' : victim_killmail.get('damage_taken'),
        'xPos' : victim_killmail['position'].get('x'),
        'yPos' : victim_killmail['position'].get('y'),
        'zPos' : victim_killmail['position'].get('z'),
        'ShipTypeID' : victim_killmail.get('ship_type_id'),
    }

    cursor.execute(victims_insert_query, victims_data)

    zkb_insert_query = ("INSERT INTO zkb "
                        "(killID, locationID, hash, fittedValue, totalValue, points, npc, solo, awox, href) "
                        "VALUES (%(killID)s,%(locationID)s, %(hash)s, %(fittedValue)s, %(totalValue)s, %(points)s, %(npc)s, %(solo)s, %(awox)s, %(href)s)")
    
    zkb_killmail = copy.deepcopy(data['zkb'])


    zkb_data = {
        'killID' : killmail_ID,
        'locationID' : zkb_killmail.get('locationID'),
        'hash' : zkb_killmail.get('hash'),
        'fittedValue'  : zkb_killmail.get('fittedValue'),
        'totalValue' : zkb_killmail.get('totalValue'),
        'points' : zkb_killmail.get('points'),
        'npc' : bit_convert(zkb_killmail.get('npc')),
        'solo' : bit_convert(zkb_killmail.get('solo')),
        'awox' : bit_convert(zkb_killmail.get('awox')),
        'href' : zkb_killmail.get('href'),
    }
    # Make sure data is committed to the database
    cnx.commit()

    cursor.close()
    cnx.close()

def main():
    while True:
        sql_connect()
        print("one update")
        
if __name__ == "__main__":
    main()
