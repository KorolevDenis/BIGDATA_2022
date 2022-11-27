
subject_dict = {
    'AGRI': 1,
    'ARTS': 2,
    'BIOC': 3,
    'BUSI': 4,
    'CENG': 5,
    'CHEM': 6,
    'COMP': 7,
    'DECI': 8,
    'DENT': 9,
    'EART': 10,
    'ECON': 11,
    'ENER': 12,
    'ENGI': 13,
    'ENVI': 14,
    'HEAL': 15,
    'IMMU': 16,
    'MATE': 17,
    'MATH': 18,
    'MEDI': 19,
    'NEUR': 20,
    'NURS': 21,
    'PHAR': 22,
    'PHYS': 23,
    'PSYC': 24,
    'SOCI': 25,
    'VETE': 26,
    'MULT': 27,
}

subject_dict_len = len(subject_dict)
proc_numb = 4
subject_len_list = [subject_dict_len // proc_numb for i in range(proc_numb)]

for ind in range(subject_dict_len % proc_numb):
    subject_len_list[ind] += 1
print(subject_len_list)

sum = 0
for i in range(0, len(subject_len_list)):
    if i == 0:
        print(f"curl http://localhost:6800/schedule.json -d project=APIScopusCrawler -d spider=scopus_api -d domains='['0', '{subject_len_list[i] - 1}']'")
    else:
        print(f"curl http://localhost:6800/schedule.json -d project=APIScopusCrawler -d spider=scopus_api -d domains='['{sum}', '{sum + subject_len_list[i] - 1}']' ")
        
    sum += subject_len_list[i]

curl http://localhost:6800/schedule.json -d project=APIScopusCrawler -d spider=scopus_api -d domains='['0', '6', '4', '0']'
curl http://localhost:6800/schedule.json -d project=APIScopusCrawler -d spider=scopus_api -d domains='['7', '13', '4', '1']' 
curl http://localhost:6800/schedule.json -d project=APIScopusCrawler -d spider=scopus_api -d domains='['14', '20', '4', '2']' 
curl http://localhost:6800/schedule.json -d project=APIScopusCrawler -d spider=scopus_api -d domains='['21', '26', '4', '3']' 