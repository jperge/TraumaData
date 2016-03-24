"""
Census data has different identifiers for gender, race and hispanic ethnicity.
I update these here to match the identifiers to the trauma DB.
As the census data contains an extra race attribute 'two or more', I include this 
in the ntdb sub_race1 and sub_race2 tables. 
Janos Perge, 2016 March
"""

import sqlite3
 
csvfile = r'C:\Users\Admin\Documents\NationalTrauma\buildDB\jpergeim3exfrc1.asc'
db = r'C:\Users\Admin\Documents\NationalTrauma\buildDB\ixNTDBshort.sqlite'

conn = sqlite3.connect(db)
conn.text_factory = str  # allows utf-8 data to be stored
c = conn.cursor()
       
tablename = 'popnational'
print 'processing ' + tablename + '...'

#%%
"""Note: qlite3, c.executescript() does not like multiple value inserts within the same command. 
E.g.: insert into censGender (genderId, genderdes) values (1, 'Male'), (2, 'Female'); reports an error of 
near ",": syntax error, while DB Browser for SQLite will let you do that without fuzz
"""

c.executescript("""

    drop table if exists censGender;
    CREATE TABLE censGender (gender_id INTEGER, genderdes VARCHAR);
    
    insert into censGender (gender_id, genderdes) values (1,'Male');
    insert into censGender (gender_id, genderdes) values (2, 'Female');
    
    UPDATE popnational
    SET sex = (SELECT censGender.genderdes
                                FROM censGender
                                WHERE censGender.gender_id = popnational.sex );
    
    UPDATE popnational
    SET sex = (SELECT sub_gender.gender_id
                                FROM sub_gender
                                WHERE sub_gender.genderdes = popnational.sex );
    drop table censGender;
    
    drop table if exists censRace;
    CREATE TABLE censRace (id INTEGER, des VARCHAR);
    
    insert into censRace (id, des) values (1,'White');
    insert into censRace (id, des) values (2, 'Black or African American');
    insert into censRace (id, des) values (3, 'American Indian');
    insert into censRace (id, des) values (4, 'Asian');
    insert into censRace (id, des) values (5, 'Native Hawaiian or Other Pacific Islander');
    insert into censRace (id, des) values (6, 'Two or More Races');
   
    insert into sub_race1 (race1_id, race1des) values (9,'Two or More Races');
    insert into sub_race2 (race2_id, race2des) values (9,'Two or More Races');
    
    UPDATE popnational
    SET race6 = (SELECT censRace.des
                                FROM censRace
                                WHERE censRace.id = popnational.race6 );
    
    UPDATE popnational
    SET race6 = (SELECT sub_race1.race1_id
                                FROM sub_race1
                                WHERE sub_race1.race1des = popnational.race6);
    drop table censRace;
    
    drop table if exists censHisp;
    CREATE TABLE censHisp (id INTEGER, des VARCHAR);
    insert into censHisp (id, des) values (1,'Not Hispanic or Latino');
    insert into censHisp (id, des) values (2, 'Hispanic or Latino');
    
    UPDATE popnational
    SET hisp = (SELECT censHisp.des
                                FROM censHisp
                                WHERE censHisp.id = popnational.hisp );
    UPDATE popnational
    SET hisp = (SELECT sub_ethnic.ethnic_id
                                FROM sub_ethnic
                                WHERE sub_ethnic.ethnicdes = popnational.hisp);
    drop table censHisp; 
    """)

conn.commit()
c.close()
conn.close()

print '... finished'
