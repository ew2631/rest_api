from aeneid.dbservices.RDBDataTable import RDBDataTable
import requests
import json

def test_create():
    tbl=RDBDataTable("people")
    print("test_create:tbl=", tbl)
    print('trying it again')

