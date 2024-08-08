# import pandas as pd
# file= open('/Users/shubhamkgupta/Desktop/Learning/KAFKA/Kafka_project/files/HealthAuth-20220915 2.txt','r')
# # file= pd.read_txt("/Users/shubhamkgupta/Desktop/Learning/KAFKA/Kafka_project/files/HealthAuth-20220915 2.txt")
def subs(text:str):
    # text=SUB21307P016       254379465425    James           S               Simmons         1255            New Street      Clark,NJ        07066
    indices=[[3,19],[19,35],[35,51],[51,67],[67,83],[83,99],[99,115],[115,131],[131,147]]
    field=['CASE_NUMBER','MEM_ID','MEM_FIRST_NAME','MEM_MIDDLE_NAME','MEM_LAST_NAME','MEM_ADD_1','MEM_ADD_2','MEM_CITY','MEM_PIN']
    parts=[str(text[index[0]:index[1]]) for index in indices]
    # print(parts)
    res = {"Subscriber":{field[i]: parts[i] for i in range(len(field))}}
    return res

def pat(text:str):
    # text=SUB21307P016       254379465425    James           S               Simmons         1255            New Street      Clark,NJ        07066
    indices=[[3,19],[19,35],[35,51],[51,67],[67,83],[83,99],[99,115],[115,131],[131,147]]
    field=['CASE_NUMBER','PAT_ID','PAT_FIRST_NAME','PAT_MIDDLE_NAME','PAT_LAST_NAME','PAT_SEX','PAT_DOB','PAT_PLANE_TYPE','PAT_PLAN_NAME']
    parts=[str(text[index[0]:index[1]]) for index in indices]
    # print(parts)
    res = {"Patient":{field[i]: parts[i] for i in range(len(field))}}
    return res

def case(text:str):
    # text=SUB21307P016       254379465425    James           S               Simmons         1255            New Street      Clark,NJ        07066
    indices=[[3,19],[19,35],[35,51],[51,67],[67,83],[83,99],[99,115]]
    field=['CASE_NUMBER','CASE_TYPE','CASE_CODE','CASE_START_DATE','CASE_END_DATE','CASE_AUTH_TYPE','CASE_STATUS']
    parts=[str(text[index[0]:index[1]]) for index in indices]
    # print(parts)
    res = {"Case":{field[i]: parts[i] for i in range(len(field))}}
    return res

def serv(text:str):
    # text=SUB21307P016       254379465425    James           S               Simmons         1255            New Street      Clark,NJ        07066
    indices=[[3,19],[19,35],[35,51],[51,67],[67,83],[83,99],[99,115]]
    field=['CASE_NUMBER','SVC_ID','SVC_TYPE','SVC_CODE','SVC_FAC_ID','SVC_PHY_ID','SVC_PHY_NAME',]
    parts=[str(text[index[0]:index[1]]) for index in indices]
    # print(parts)
    res = {"subscriber":{field[i]: parts[i] for i in range(len(field))}}
    return res
# print(file.readline())

# def subs(text:str):
#     text=SUB21307P016       254379465425    James           S               Simmons         1255            New Street      Clark,NJ        07066
#     indices=[[3,19],[19,35],[35,51],[51,67],[67,83],[83,99],[99,115],[115,131],[131,148]]
#     field=['CASE_NUMBER','MEM_ID','MEM_FIRST_NAME','MEM_MIDDLE_NAME','MEM_LAST_NAME','MEM_ADD_1','MEM_ADD_2','MEM_CITY','MEM_PIN']
#     parts=[text[index[0]:index[1]] for index in indices]


# def extractor(text1:str,text2:str,text3:str,text4:str):
#     if(text1.startswith)
#
#         s = 'long string that I want to split up'
#         indices = [
#             0, 5, 12, 17]
#         parts = [s[index:] for index in indices]
#         for part in parts:
#             print
#             part

def main(text:str):
    if(text.startswith('SUB')):
        return subs(text)
    elif(text.startswith('PAT')):
        return pat(text)
    elif (text.startswith('CAS')):
        return case(text)
    elif (text.startswith('SVC')):
        return serv(text)



