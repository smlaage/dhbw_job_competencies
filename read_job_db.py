"""
read_job_db.py: Creates a full extract from table jobs. Saves the data to a pandas dataframe.

SLW - 07-2022
"""

import time
import mysql.connector
import pandas as pd
import numpy as np
import unicodedata


def rem_punctuation(s):
    """ Removes punctuation charcaters from a string
        Changes the case to casefold"""
    punctuation = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~\t'
    umlaute = ((unicodedata.normalize('NFKD', 'ä'), unicodedata.normalize('NFKD', 'ae')),
               (unicodedata.normalize('NFKD', 'ö'), unicodedata.normalize('NFKD', 'oe')),
               (unicodedata.normalize('NFKD', 'ü'), unicodedata.normalize('NFKD', 'ue')))
    if s == "NA":
        return ""
    new_str = s.casefold()
    new_str = unicodedata.normalize("NFKD", new_str)
    for c in s:
        if c in punctuation:
            new_str = new_str.replace(c, ' ')
    new_str = new_str.replace('\n', ' ')
    while '  ' in new_str:
        new_str = new_str.replace('  ', ' ')
    for um in umlaute:
        new_str = new_str.replace(um[0], um[1])
    if new_str[0] != ' ': new_str = ' ' + new_str
    if new_str[-1] != ' ': new_str = new_str + ' '
    return new_str


def extract_year_month(dt):
    """ Extracts year and month from datetime. Returns a string with year and month. """ 
    return "{:04d}-{:02d}".format(dt.year, dt.month)


def read_job_db():
    """ Extracts the raw data from the SQL database. Adds column for week """
    print("Reading job db ...")
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = "jobs_at_dhbw",
        database = "job_db",
        charset = 'utf8'
    )
    print("- connected to:", mydb)
    mycursor = mydb.cursor()
    sql = "SELECT job_id, title, location, company, job_dt, introduction, "
    sql += "description, profile, weoffer, contract_type, work_type, pers_resp "
    sql += "FROM jobs"
    mycursor.execute(sql)
    jobs = pd.DataFrame(mycursor.fetchall())
    mydb.close()
    print("- transforming data ...")
    jobs.columns = ['job_id', 'title', 'location', 'company', 'job_dt',
                    'introduction', 'description', 'profile', 'weoffer',
                    'contract_type', 'work_type', 'pers_resp']
    jobs = jobs.set_index('job_id')
    print("    : introduction")
    jobs['introduction_clean'] = jobs['introduction'].apply(rem_punctuation)
    print("    : description")
    jobs['description_clean'] = jobs['description'].apply(rem_punctuation)
    print("    : profile")
    jobs['profile_clean'] = jobs['profile'].apply(rem_punctuation)
    print("    : weoffer")
    jobs['weoffer_clean'] = jobs['weoffer'].apply(rem_punctuation)
    jobs.insert(4, 'year_month', jobs['job_dt'].apply(extract_year_month))
    print("Done")
    print()
    return jobs

#==============================================================================

jobs = read_job_db()

print()
t = time.localtime()
filename = "jobs-{:04d}-{:02d}-{:02d}-{:02d}-{:02d}-{:02d}.csv".format(
    t[0], t[1], t[2], t[3], t[4], t[5])
print("Saving data to file " + filename)
jobs.to_csv(filename, encoding='UTF-8', escapechar='\\')
print("Done")

