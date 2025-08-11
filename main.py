import pandas as pd
import os
import subprocess as sb
import mysql.connector

filename = input("Enter the name of the excel file: ")
file_path = input("Enter the path to the specified file: ")

# filename = "excelfile.xlsx"
# file_path = "C:\Users\lenovo\Desktop\Coding\Python\PythonProjects\exceldbgen"

auto_conf = input("Do you want to use auto config? (y/n): ")

if auto_conf == "y":
    auto_conf = True
else:
    auto_conf = False

if file_path.find('/')==-1:
    file_path.replace('/', '\\')

file = os.path.join(file_path, filename)

cwd = os.getcwd()

# --------------Checking if MySQL is installed----------------

os.chdir("C:/Program Files")
sqlcheck1 = sb.check_output("dir", shell=True, universal_newlines=True)
os.chdir("C:/Program Files (x86)")
sqlcheck2 = sb.check_output("dir", shell=True, universal_newlines=True)
os.chdir("C:/ProgramData")
sqlcheck3 = sb.check_output("dir", shell=True, universal_newlines=True)

sqlcheck = sqlcheck1+" "+sqlcheck2+" "+sqlcheck3

os.chdir(cwd)

# print(sqlcheck.lower())

if "mysql" not in sqlcheck.lower():
    print("MySQL installation not found. Please try again after installing MySQL from https://dev.mysql.com/downloads/installer")

# ---------------Done Checking---------------

# ---------------Main functionality------------------ 

data = pd.read_excel(file)

columns = list(data.columns)

# Table Columns

tb_columns = []

if auto_conf:
    for i in columns:
        if i == "":
            break
        else:
            tb_columns.append(i)

    table_data = []

    for i in tb_columns:
        table_data.append(data[i].tolist())

    for i in table_data:
        if i == list(dict.fromkeys(i)):
            pk = table_data.index(i)
            break
        else:
            continue

    print(pk)

    old_conf_data = []

    for j in enumerate(table_data):
        old_conf_data.append(['varchar(255)'])

    old_conf_data[pk] = ["double", "primary key", "not null"]

    # Rmoving nan

    def removeBlank(x):
        if str(x)=="":
            return False
        else:
            return True

    for i in old_conf_data:
        for j in i:
            if str(j) == "nan":
                i[i.index(j)] = ""

    for i in table_data:
        for j in i:
            if str(j) == "nan":
                i[i.index(j)] = "NULL"

    conf_data = []

    for i in old_conf_data:
        h = list(filter(removeBlank, i))
        conf_data.append(h)
    
else:
    for i in columns:
        if i == "CONFIG":
            break
        else:
            tb_columns.append(i)

# Table Data

    table_data = []

    for i in tb_columns:
        table_data.append(data[i].tolist())

    # Conf columns

    def checkConf(item):
        return item.find("-conf") != -1

    conf_columns = list(filter(checkConf, columns))

    # Conf Data

    old_conf_data = []

    for i in conf_columns:
        old_conf_data.append(data[i].tolist())

    # Rmoving nan

    def removeBlank(x):
        if str(x)=="":
            return False
        else:
            return True

    for i in old_conf_data:
        for j in i:
            if str(j) == "nan":
                i[i.index(j)] = ""

    for i in table_data:
        for j in i:
            if str(j) == "nan":
                i[i.index(j)] = "NULL"

    conf_data = []

    for i in old_conf_data:
        h = list(filter(removeBlank, i))
        conf_data.append(h)

print(columns)
print(table_data)
print(conf_data)

# ----------------------Constructing Database--------------------------
database = input("Enter the name of the database: ")
table = input("Enter the name of the table: ")
username = input("Enter your MySQL Username (leave blank if default): ")
my_password = input("Enter your MySQL Password: ")

if len(username)==0:
    username = "root"

mydb = mysql.connector.connect(host='localhost', user=username, password=my_password)
# print(mydb.connection_id) JUST FOR CHECKING...
cursor = mydb.cursor()

try:
    cursor.execute(f"CREATE DATABASE {database};")
except:
    pass

cursor.execute(f"USE {database};")

# Creating table & Describing its Properties

tb_values = "("

for i,j in zip(tb_columns, conf_data):
    j = str(j)
    j = j.replace("'", "")
    j = j.replace("[", "")
    j = j.replace("]", "")
    j = j.replace(",", "")
    tb_values += i + " " +j + ", "

tb_values = tb_values[:-2]+")"

try:
    cursor.execute(f"CREATE TABLE {table} {tb_values};")
except:
    cursor.execute(f"DROP TABLE {table};")
    cursor.execute(f"CREATE TABLE {table} {tb_values};")

# Inserting data into table

arg_tb_data = [tuple([sublist[i] for sublist in table_data]) for i in range(len(table_data[0]))]

for i in arg_tb_data:
    value = str(i)
    value = value.replace("'NULL'", "NULL")
    cursor.execute(f"INSERT INTO {table} VALUES {value};")

mydb.commit()

print()