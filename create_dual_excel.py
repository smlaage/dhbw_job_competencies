"""
create_dual_excel.py: Reads the job data from csv file,
searches for the keyword 'dual' and creates an excel file as output.

SLW - 03-2024
"""

import time
import pandas as pd
import unicodedata
import openpyxl


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


def find_dual(jobs, keywords=['dual'], col='title'):
    """ Scans the job dataframe for keywords """
    print("Findung jobs for dual students ...")
    jobs_dual = []
    for index, row in jobs.iterrows():
        title = rem_punctuation(row[col])
        for kw in keywords:
            if title.find(kw) >= 0:
                jobs_dual.append(list(row))
                break
        if index % 10000 == 0:
            print(index, end='\r')
    jobs_dual = pd.DataFrame(jobs_dual)
    print(" -" len(jobs_dual), "jobs found")
    if len(jobs_dual) > 0:
        jobs_dual.columns = jobs.columns
        for c in jobs_dual.columns:
            if c.find('clean') > 0:
                jobs_dual = jobs_dual.drop(c, axis=1)
        jobs_dual = jobs_dual.drop('pers_resp', axis=1)   
    return jobs_dual

#==============================================================================

filename = "jobs-2024-03-25-10-48-02.csv"
excel_file = "jobs_dual.xlsx"

print("Reading data from '" + filename + "'")
jobs = pd.read_csv(filename, encoding = "UTF-8", nrows=100000)
jobs_dual = find_dual(jobs)
print("Saving data to '" + excel_file + "'") 
with pd.ExcelWriter(excel_file) as writer:
    jobs_dual.to_excel(writer)
    
print("Done")