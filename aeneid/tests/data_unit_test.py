from aeneid.dbservices.RDBDataTable import RDBDataTable
import logging
logging.basicConfig(level=logging.DEBUG)
from aeneid.dbservices import dataservice as ds

def create_rdb_test():
    tbl=RDBDataTable("people",key_columns="cat")
    print("'create_rdb test", tbl)

def create_fantasy_manager():
    tbl = RDBDataTable("people", key_columns="cat")
    r=tbl.insert({"playerID":"aaa1", "birthYear":"1943","birthMonth":"2"})
    print('create_fantasy_manager returned: ', tbl)

'''def create_fantasy_manager():
    result=ds.create("moneyball": "fantasy_manager",
                                  {"id": "dff10","last name": "Ferguson", "first_name":
                                      "Douglas", "email": "dff1@columbia.edu"}
                    )
    print('create_fantasy_manager returned: ', result)'''

#create_rdb_test()
create_fantasy_manager()

