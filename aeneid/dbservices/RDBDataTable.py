# Emily Wang-ew2631
from aeneid.dbservices.BaseDataTable import BaseDataTable
from aeneid.dbservices.DerivedDataTable import DerivedDataTable
import aeneid.dbservices.dataservice as ds


import pymysql
import pandas as pd
import sys
from operator import itemgetter

sys.path.append('/Users/Emily/Documents/github/w4111-Databases/HW_Assignments/HW1/src')

class RDBDataTable(BaseDataTable):
    """
    RDBDataTable is relation DB implementation of the BaseDataTable.
    """

    # Default connection information in case the code does not pass an object
    # specific connection on object creation.
    _default_connect_info = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Lotuslesson*7258',
        'db': 'lahman2017n',
        'port': 3306
    }

    def debug_message(self, *m):
        """
        Prints some debug information if self._debug is True
        :param m: List of things to print.
        :return: None
        """
        if self._debug:
            print(" *** DEBUG:", *m)

    def __init__(self, table_name, key_columns, connect_info=None, debug=True):
        """

        :param table_name: The name of the RDB table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        # Initialize and store information in the parent class.
        super().__init__(table_name, connect_info, key_columns, debug)

        # If there is not explicit connect information, use the defaults.
        if connect_info is None:
            self._connect_info = RDBDataTable._default_connect_info

        # Create a connection to use inside this object. In general, this is not the right approach.
        # There would be a connection pool shared across many classes and applications.
        self._cnx = pymysql.connect(
            host=self._connect_info['host'],
            user=self._connect_info['user'],
            password=self._connect_info['password'],
            db=self._connect_info['db'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        self._related_resources = None

        split_name = table_name.split(".")
        self._schema = split_name[0]
        self._table = split_name[1]
        self._related_resources=None

        self._table_name = table_name
        self._load_foreign_key_info()

        if self._key_columns is not None:
            print("Thank you for your input, I'll find the keys")
        self._key_columns=self._get_primary_key_definition()


    def __str__(self):
        """

                :return: String representation of the data table.
                """
        result = "RDBDataTable: table_name = " + self._table_name
        result += "\nKey fields: " + str(self._key_columns)

        # Find out how many rows are in the table.
        q1 = "SELECT count(*) as count from " + self._table_name
        r1 = self._run_q(q1, fetch=True, commit=True)
        result += "\nNo. of rows = " + str(r1[0]['count'])

        # Get the first five rows and print to show sample data.
        # Query to get data.
        q = "select * from " + self._table_name + " limit 5"

        # Read into a data frame to make pretty print easier.
        df = pd.read_sql(q, self._cnx)
        result += "\nFirst five rows:\n"
        result += df.to_string()

        return result

    def _run_q(self, q, args=None, fields=None, fetch=True, cnx=None, commit=True):
        """

        :param q: An SQL query string that may have %s slots for argument insertion. The string
            may also have {} after select for columns to choose.
        :param args: A tuple of values to insert in the %s slots.
        :param fetch: If true, return the result.
        :param cnx: A database connection. May be None
        :param commit: Do not worry about this for now. This is more wizard stuff.
        :return: A result set or None.
        """
        # Use the connection in the object if no connection provided.
        if cnx is None:
            cnx = self._cnx

        # Convert the list of columns into the form "col1, col2, ..." for following SELECT.
        if fields:
            q = q.format(",".join(fields))

        cursor = cnx.cursor()  # Just ignore this for now.

        # If debugging is turned on, will print the query sent to the database.
        self.debug_message("Query = ", cursor.mogrify(q, args))

        cnt=cursor.execute(q, args)  # Execute the query.

        # Technically, INSERT, UPDATE and DELETE do not return results.
        # Sometimes the connector libraries return the number of created/deleted rows.
        if fetch:
            r = cursor.fetchall()  # Return all elements of the result.
        else:
            r = cnt

        if commit:                  # Do not worry about this for now.
            cnx.commit()
        return r

    def _run_insert(self, table_name, column_list, values_list, cnx=None, commit=True):
        """

        :param table_name: Name of the table to insert data. Probably should just get from the object data.
        :param column_list: List of columns for insert.
        :param values_list: List of column values.
        :param cnx: Ignore this for now.
        :param commit: Ignore this for now.
        :return:
        """
        try:
            q = "insert into " + table_name + " "

            # If the column list is not None, form the (col1, col2, ...) part of the statement.
            if column_list is not None:
                q += "(" + ",".join(column_list) + ") "

            # We will use query parameters. For a term of the form values(%s, %s, ...) with one slot for
            # each value to insert.
            values = ["%s"] * len(values_list)

            # Form the values(%s, %s, ...) part of the statement.
            values = " ( " + ",".join(values) + ") "
            values = "values" + values

            # Put all together.
            q += values

            result=self._run_q(q, args=values_list, fields=None, fetch=False, cnx=cnx, commit=commit)
            return result
        except Exception as e:
            print("Got exception = ", e)
            raise e

    def _get_primary_key(self):
        q="SHOW KEYS FROM " +self._table_name+ " WHERE Key_name='PRIMARY'"
        rows=self._run_q(q=q, args=None)
        keys=[r["Column_name"] for r in rows]
        return keys

    def _get_primary_key_columns(self):
        return self._key_columns

    def _get_primary_key_values(self, r):
        try:
            result={k:r[k] for k in self._key_columns}
        except KeyError:
            result=None
        return result

    def _get_primary_key_definition(self):

        keys = None

        q = "SHOW KEYS FROM " + self._table_name + " WHERE Key_name = 'PRIMARY'"
        rows = self._run_q(q=q, args=None, fetch=True)

        if rows and len(rows) > 0:
            keys = [[r["Column_name"], r['Seq_in_index']] for r in rows]
            keys = sorted(keys, key=itemgetter(1))
            keys = [k[0] for k in keys]

        return keys

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the request fields for the record identified
            by the key.
        """
        key_columns=self._key_columns
        if (not key_columns) or len(key_columns)==0:
            #logging.exception(self._table_name + "doesn't have a primary key")
            e = ValueError("Bad keys")
            raise e
        tmp = dict(zip(key_columns, key_fields))
        result = self.find_by_template(tmp, field_list=field_list)
        if result is not None:
            result=result.get_rows()
            if result is not None and len(result)>0:
                result=result[0]
            else:
                result=None
        return result


    def _template_to_where_clause(self, t):
        """
        Convert a query template into a WHERE clause.
        :param t: Query template.
        :return: (WHERE clause, arg values for %s in clause)
        """
        terms = []
        args = []
        w_clause = ""

        # The where clause will be of the for col1=%s, col2=%s, ...
        # Build a list containing the individual col1=%s
        # The args passed to +run_q will be the values in the template in the same order.
        for k, v in t.items():
            temp_s = k + "=%s "
            terms.append(temp_s)
            args.append(v)

        if len(terms) > 0:
            w_clause = "WHERE " + " AND ".join(terms)
        else:
            w_clause = ""
            args = None

        return w_clause, args

    def  _get_extras(self, limit=None, offset=None, order_by=None):
        result=' '
        if order_by:
            result += ' order by ' + str(order_by)
        if limit:
            result+=' limit ' + str(limit)
        if offset:
            result+=' offset ' + str(offset)

        return result

    def find_by_template(self, template, field_list=None, commit=True,limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        result=None
        try:
            wc=self._template_to_where_clause(template)
            ext = self._get_extras(limit, offset, order_by)
            if field_list is None:
                f_select=['*']

            else:
                f_select=field_list
            q = "select {} from " + self._table_name + " " + wc[0] + " " + ext


            result=self._run_q(q,args=wc[1], fields=f_select, commit=True, fetch=True)
            if result and len(result) > 0:
                result=DerivedDataTable("FBT:"+ self._table_name,result)
            else:
                result=None
            return result
        except Exception as e:
            print("RDBDataTable.find_by_template: Exception=", e)
            raise e

    def _row_to_key_string(self, row):

        result = [row[k] for k in self._key_columns]
        result = "_".join(result)
        return result

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: the primary key field and value
        """
        try:
            c_list = list(new_record.keys())
            v_list = list(new_record.values())
            self._run_insert(self._table_name, c_list, v_list)
            return self._get_primary_key_values(new_record)
            #return self._row_to_key_string(new_record)
        except Exception as e:
            print("insert: Exception e = ", e)
            raise(e)

    def delete_by_template(self, template):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        wc = self._template_to_where_clause(template)
        q="delete from " + self._table_name + " "+wc[0]
        result=self._run_q(q,args=wc[1],fetch=False)
        return result

    def delete_by_key(self, key_fields):
        """

        Delete record with corresponding key.

        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        try:
            k=dict(zip(self._key_columns,key_fields))
            return self.delete_by_template(k)
        except Exception as e:
            ValueError("RDBDataTable.delete_by_key exception")
            raise e

    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        terms=[]
        set_args=[]
        for k, v in new_values.items():
            terms.append(k+"=%s")
            set_args.append(v)
        terms=",".join(terms)
        wc=self._template_to_where_clause(template)
        set_args.extend(wc[1])
        q="update "+self._table_name + " set "+ str(terms) + " " + wc[0]
        result=self._run_q(q, set_args, fetch=False)
        return result

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """

        tmp = dict(zip(self._key_columns, key_fields))
        return self.update_by_template(tmp,new_values)

    def insert_related(self,key,new_row,related_resource):
        cols = self._get_primary_key_definition()
        k = dict(zip(cols, key))
        related_tbl=ds.get_data_table(related_resource)
        mapped_key = self._map_key(related_resource, k)
        r2 = {**mapped_key, **new_row}
        result = related_tbl.insert(r2)
        return result

    def find_related(self, key, child_resource, field_list=None, limit=None, offset=None, order_by=None):
        result=None
        try:
            #tmp=self._key_to_template(key)
            cols = self._get_primary_key_definition()
            k = dict(zip(cols, key))
            mapped_key=self._map_key(child_resource,k)
            if mapped_key is None:
                row=self.find_by_primary_key(key)
                mapped_key=self._map_key(row,child_resource)
            if mapped_key is None:
                raise ValueError("No key")
            if "." not in child_resource:
                child_table_name=self._schema+ "." + child_resource
            else:
                child_table_name=child_resource
            child_table=ds.get_data_table(child_table_name)
            result=child_table.find_by_template(mapped_key,field_list,limit=limit, offset=offset,order_by=order_by)
        except Exception as e:
            raise ValueError("RDBDataTable.find_by_path_key doesn't work")
        return result


    def _key_to_template(self, key):
        result=dict(zip(self._key_columns,key))
        return result

    def _get_map(self, other_resource):

        result = None

        split_n = other_resource.split(".")
        if len(split_n) > 1:
            r = split_n[1]
        else:
            r = split_n[0]

        for k,v in self._related_resources.items():

            if (v['TABLE_NAME'].lower() == self._table and v['REFERENCED_TABLE_NAME'].lower() == r) \
                or \
                (v['TABLE_NAME'].lower() == r and v['REFERENCED_TABLE_NAME'].lower() == self._table):
                result = self._related_resources[k]
                break

        return result

    def _map_key(self, other_resource, key):
        result = {}

        map = self._get_map(other_resource)

        for m in map['MAP']:
            if map['to_me'] == True:
                my_col = m[1]
                other_col = m[0]
            else:
                my_col = m[0]
                other_col = m[1]

            result[other_col] = key[my_col]

        return result

    def get_primary_key_value(self, r):
        try:
            result={k:r[k] for k in self._key_columns}
        except KeyError:
            result=None
        return result

    def get_primary_key_columns(self):
        return self._key_columns

    def _load_foreign_key_info(self):
        """

        :return: Loads the foreign keys to this table and from this table to other tables.
        """

        result = None
        result = self._related_resources  # Do not load if we have already loaded.

        if result is None:  # Do not load if we have already loaded.

            schema_name = self._schema  # Schema for this table.

            # If this table is the source of the foreign key, then the column is table_name.
            # If this table is the target of the forign key, then the columns is referenced_table_name
            # Only select rows where this table is the source or target.
            q = """
            SELECT
              CONSTRAINT_NAME,
              TABLE_SCHEMA,
              TABLE_NAME,
              COLUMN_NAME,
              REFERENCED_TABLE_NAME,
              REFERENCED_TABLE_SCHEMA,
              REFERENCED_COLUMN_NAME,
              ORDINAL_POSITION, 
              POSITION_IN_UNIQUE_CONSTRAINT
                FROM
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE
                    REFERENCED_TABLE_SCHEMA = %s
                    AND
                        ((table_name=%s) or (REFERENCED_TABLE_NAME=%s))"""

            # For W4111 homework, an example would be ('lahman2017', 'people', 'people') to
            # find foreign keys referencing this table.
            args = (self._schema, self._table, self._table)

            result = self._run_q(q, args)

            paths = None

            # Did we get results?
            if result is not None and len(result) > 0:

                # This will be a dictionary holding the constraint information. The key
                # is the name of the constraint.
                paths = {}

                for r in result:

                    # Get the entry in the dictionary, get it, otherwise return none.
                    p = paths.get(r['CONSTRAINT_NAME'], None)

                    # This is the first time we have seen this constraint.
                    if p is None:

                        # If the REFERENCED_TABLE_NAME is my table name, then the constraint is to me.
                        if r['REFERENCED_TABLE_NAME'].lower() == self._table:
                            to_me = True
                        else:
                            to_me = False

                        # Make a new entry in the dictionary.
                        p = {}
                        p['to_me'] = to_me
                        p['MAP'] = []
                        p['CONSTRAINT_NAME'] = r['CONSTRAINT_NAME']
                        p['TABLE_NAME'] = r['TABLE_NAME']
                        p['TABLE_SCHEMA'] = r['TABLE_SCHEMA']
                        p['REFERENCED_TABLE_NAME'] = r['REFERENCED_TABLE_NAME']
                        p['REFERENCED_TABLE_SCHEMA'] = r['REFERENCED_TABLE_SCHEMA']

                        # Add to the dictionary
                        paths[r['CONSTRAINT_NAME']] = p

                    # At this point, we either found or created an entry in the dictionary.
                    # Save the column mapping. There may be many columns. So, the mapping is a list.
                    t = (r['COLUMN_NAME'], r['REFERENCED_COLUMN_NAME'])
                    p['MAP'].append(t)

            # Set the result in the class instance data.
            self._related_resources = paths

    def _get_related_resources_names(self):
        resources=self._related_resources
        table_name=self._table
        ans=[]
        for k,v in resources.items():
            test=resources[k]['TABLE_NAME'].lower()
            if resources[k]['TABLE_NAME'].lower() != table_name:
                ans.append(resources[k]['TABLE_NAME'].lower())
        return ans
    #def get_related_resource_names(self):
        #related_resources= self._related_resources

    def _get_specific_template(self, parent_resource, template):
        parent=parent_resource.split(".")
        parent=parent[1]
        ans={}
        for i in template.items():
            lst = i[0].split(".")
            tbl_name = lst[0]
            if tbl_name == parent:
                ans.update([i])
        return ans

    def find_by_path_template(self, parent_resource, child_resources=None,template=None, \
                               field_list=None, limit=None, offset=None, order_by=None):
        child_resource_list=None
        field_array=None
        parent=parent_resource.split(".")
        parent=parent[1]
        if child_resources:
            child_resource_list=child_resources.split(",")
        field_array=field_list
        if child_resource_list is None:
            return self.find_by_template(template, field_list, limit, offset, order_by)
        else:
            result=None
            parent_template=self._get_specific_template(parent_resource , template)
            parent_fields=[f for f in field_array if (parent + ".") in f]
            for c in child_resource_list:
                child_template=self._get_specific_template(c, template)
                child_fields=[f for f in field_array if (c+".") in f]
                if child_template is not None:
                    all_template={**parent_template, **child_template}
                else:
                    all_template=parent_template

            all_fields=",".join(parent_fields+child_fields)
            t=self.find_by_path_template_pair(parent_resource, c, all_template, all_fields, limit, offset, order_by)
            if result is None:
                result=t
            else:
                for sub_r in result:
                    for sub_t in t:
                        rp_s=str(sub_r[parent_resource])
                        tp_s=str(sub_r[parent_resource])
                        if tp_s == rp_s:
                            sub_r[c]=sub_t[c]
                            break


    def find_by_path_template_pair(self, parent_resource, child_resource=None, template=None,
                                    field_list=None, limit=None, offset=None, order_by=None):
        result=None
        jc=[]
        try:
            q="select {columns}\n from {tables}\n {on} \n {where} \n {extras}"
            on_c=None
            if child_resource:
                field_list=self._add_aliases(field_list)
            jc=self._get_join_clause(parent_resource, child_resource)
            on_c=jc
            tables= " " + parent_resource + "," + child_resource + " "
            wc=self._template_to_where_clause(template)
            if on_c is not None:
                w_string=wc[0]+ " and " + on_c
            else:
                on_c = " "
                w_string=wc[0]
            q=q.format(columns=field_list, tables=tables, on= " ",where=w_string,extras=None)
            result=self._run_q(q, wc[1],fetch=True)
            return result
        except Exception as e:
            print("fine_by_path_template exception= ", e)



    def _add_aliases(self, fields):
        field_list = fields.split(",")
        alias = field_list[0] + " as " + field_list[0]
        field_list = field_list[1:]
        for i in field_list:
            alias = alias + ","
            alias = alias + i + " as " + i
        return alias
    def _get_join_clause(self, parent_resource, child_resource):
        result_terms=[]
        for k,v in self._related_resources.items():
            if (v["TABLE_NAME"].lower()==parent_resource and v["REFERENCED_TABLE_NAME"].lower()==child_resource) \
                or \
                    (v["TABLE_NAME"].lower()== child_resource and v["REFERENCED_TABLE_NAME"].lower()==parent_resource):
                for me in v['MAP']: \
                    result_terms.append(v['TABLE_NAME']+"."+me[0]+"="+ v['REFERENCED_TABLE_NAME']+"."+me[1])
                break
        if result_terms:
            result=" AND ".join(result_terms)
        else:
            result=None
        return result


'''import json
tbl = RDBDataTable('lahman2017n.teams', None)
related={}
if 'Resources' in related.keys():
    print("Hello")
else:
    print("Good bye")
k=tbl._get_related_resources_names()
related['Resources']=k
for i in related['Resources']:
    print(i)
print ("Related=",json.dumps(related, indent=2) )


# /api/lahman2017/teams/BOS_1960/batting

def test1():
    q = "select * from batting where teamid='BOS' and yearid='1960'"
    result = tbl._run_q(q)
    print("Result = ", json.dumps(result, indent=2, default=str))

def test5():
    k = tbl._get_map("batting")
    print("Map = ", json.dumps(k, indent=2))
def test7():
    m_key = {"teamID": "BOS", "yearID": "1960"}
    result = tbl.find_by_path(m_key, "lahman2017n", "batting")
    print("Result = ", result.get_rows())

#test1()
#test5()
#test7()'''
