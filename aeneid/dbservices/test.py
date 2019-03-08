def _get_specific_template(parent_resource, template):
    ans={}
    for i in template.items():
        lst=i[0].split(".")
        tbl_name=lst[0]
        if tbl_name==parent_resource:
            ans.update([i])
    return ans

print(_get_specific_template('people', {'people.nameLast':'williams', 'people.yearid':'1960'}))

def add_alias (fields):
    field_list=fields.split(",")
    alias=field_list[0] + " as " + field_list[0]
    field_list=field_list[1:]
    for i in field_list:
        alias=alias + ","
        alias= alias+ i + " as " + i
    return alias


print(add_alias('people.playerID, team.playerID, people.nameLast'))