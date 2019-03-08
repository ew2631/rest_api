import requests
import json

#WORKS
def test_api_1():
    r=requests.get("http://127.0.0.1:5000")
    result=r.text
    print("First REST API returned",r.text)

#WORKS
def display_response(rsp):

    try:
        print("Printing a response.")
        print("HTTP status code: ", rsp.status_code)
        h = dict(rsp.headers)
        print("Response headers: \n", json.dumps(h, indent=2, default=str))

        try:
            body = rsp.json()
            print("JSON body: \n", json.dumps(body, indent=2, default=str))
        except Exception as e:
            body = rsp.text
            print("Text body: \n", body)

    except Exception as e:
        print("display_response got exception e = ", e) # #WOR

#WORKS
def test_json_2(): #just tests the explain method
    url='http://127.0.0.1:5000/explain/body'
    data={"p":"cool"}
    headers={'Content-Type':'application/json; charset=utf-8'}
    r=requests.post(url,headers=headers, json=data)
    print("Result=")
    print(json.dumps(r.json(),indent=2, default=str))

def collection_get(): #tests GET for handle_collection (URL ends in a resource, returns values)
    params={"nameLast": "Williams", "fields": "playerID, nameLast, nameFirst"}
    url='http://127.0.0.1:5000/api/lahman2017/people'
    headers={'Content-Type':'application/json; charset=utf-8'}
    r=requests.get(url,headers=headers, params=params)
    display_response(r)

def resource_get1(): #test GET method in handle_resource
    try:

        team_id=27
        sub_resource = "fantasy_manager"

        print("\ntest_get_by_path: test 1")
        print("team_id = ", team_id)
        print("sub_resource = ", sub_resource)

        path_url = "http://127.0.0.1:5000/api/moneyball/fantasy_team/" + str(team_id) + "/" + sub_resource
        print("Path = ", path_url)
        result = requests.get(path_url)
        print("test_get_by_path: path_url = ")
        display_response(result)


    except Exception as e:
        print("PUT got exception = ", e)

def resource_put(): #tests PUT for handle_resource
    try:

        print("\ntest_update_manager: test 1, get manager with id = ls1")
        url = "http://127.0.0.1:5000/api/lahman2017/baseball/ls1"
        result = requests.get(url)
        print("test_update_manager: get result = ")
        display_response(result)

        body = {
            "last_name": "Darth",
            "first_name": "Vader",
            "email": "dv@deathstar.navy.mil"
        }

        print("\ntest_update_manager: test 2, updating data with new value = ")
        print(json.dumps(body))
        headers = {"Content-Type": "application/json"}
        result = requests.put(url, headers=headers, json=body)
        print("\ntest_update_manager: test 2, updating response =  ")
        display_response(result)

        print("\ntest_update_manager: test 3, get manager with id = ls1")
        url = "http://127.0.0.1:5000/api/moneyball/fantasy_manager/ls1"
        result = requests.get(url)
        print("test_update_manager: get result = ")
        display_response(result)

    except Exception as e:
        print("PUT got exception = ", e)

def test_resource_delete(): #tests PUT for handle_resource
    try:

        url = "http://127.0.0.1:5000/api/lahman2017n/allstarfullfixed/aaronha01_ALS195807080"
        result = requests.get(url)
        print("test_delete: get result before delete = ")
        display_response(result)

        requests.delete(url, headers=headers, json=body)

        print("\ntest_delete: get team after delete")
        url = "http://127.0.0.1:5000/api/lahman2017n/allstarfullfixed/aaronha01_ALS195807080"
        result = requests.get(url)
        print("test_delete: get result = ")
        display_response(result)

    except Exception as e:
        print("PUT got exception = ", e)

def collection_post(): #tests POST for handle_collection (create a new table)
    try:
        body = {
            "id": "ok1",
            "last_name": "Obiwan",
            "first_name": "Kenobi",
            "email": "ow@jedi.org"
        }
        print("\ntest_create_manager: test 1, manager = \,", json.dumps(body, indent=2, default=str))
        url = "http://127.0.0.1:5000/api/moneyball/fantasy_manager"
        headers = {"content-type": "application/json"}
        result = requests.post(url, headers=headers, json=body)
        display_response(result)

        print("\ntest_create_manager: test 2 retrieving created manager.")
        link = result.headers.get('Location', None)
        if link is None:
            print("No link header returned.")
        else:
            url = link
            headers = None
            result = requests.get(url)
            print("\ntest_create_manager: Get returned: ")
            display_response(result)

        print("\ntest_create_manager: test 1, creating duplicate = \,", json.dumps(body, indent=2, default=str))
        url = "http://127.0.0.1:5000/api/moneyball/fantasy_manager"
        headers = {"content-type": "application/json"}
        result = requests.post(url, headers=headers, json=body)
        display_response(result)

    except Exception as e:
        print("POST got exception = ", e)

def collection_post_2(): #tests POST for handle_collection (table/key/table) for manager ls1 in
    # fantasy_manager, create a fantasy_team
    try:

        playerid = 'ls1'
        sub_resource = "fantasy_team"

        print("\ntest_create_related: test 1")
        path_url = "http://127.0.0.1:5000/api/moneyball/fantasy_manager/ls1/fantasy_team"
        print("Path = ", path_url)
        body = {"team_name": "Braves"}
        print("Body = \n", json.dumps(body, indent=2))
        result = requests.post(path_url, json=body, headers={"Content-Type" : "application/json"})
        print("test_create_related response = ")
        display_response(result)

        l = result.headers.get("Location", None)
        if l is not None:
            print("Got location = ", l)
            print("test_create_related, getting new resource")
            result = requests.get(l)
            display_response(result)
        else:
            print("No location?")


    except Exception as e:
        print("POST got exception = ", e)

#THE LAST TEST I'M MISSING IS DELETE
#r=requests.get('https://api.github.com/events')
#display_response(r)

collection_get()

#test_resource_delete()