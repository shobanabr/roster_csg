import boto3
import csv
import random
from datetime import datetime

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Table where final output is stored
table = dynamodb.Table("table_name")
 
def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = 'file1.csv'
    s3_file_name1 = 'file2.csv'
    s3_file_name2 = 'file3.csv'    
    resp = s3_client.get_object(Bucket=bucket_name,Key=s3_file_name)
    resp1 = s3_client.get_object(Bucket=bucket_name,Key=s3_file_name1) 
    resp2 = s3_client.get_object(Bucket=bucket_name,Key=s3_file_name2)

    data1 = resp['Body'].read().decode("utf-8")   #file1
    data2 = resp1['Body'].read().decode("utf-8") #file2 
    data3 = resp2['Body'].read().decode("utf-8") #employee list

    iterations = ['24.1','24.2','24.3','24.4']
    iterations_prod = {'24.1':'03/16/2024','24.2':'06/22/2024', 
                       '24.3':'09/14/2024','24.4':'12/07/2024'}
    iterations_cte  = {'24.1':'02/20/2024','24.2':'05/28/2024',
                       '24.3':'08/27/2024','24.4':'11/26/2024'}

    team1 = "team1"
    team2 = "team2"
    team3 = "team3"
    
    # convert PROD date to mm/dd/yyyy
    iteration1_pdate = datetime.strptime(iterations_prod['24.1'], "%m/%d/%Y").date()
    iteration2_pdate = datetime.strptime(iterations_prod['24.2'], "%m/%d/%Y").date()
    iteration3_pdate = datetime.strptime(iterations_prod['24.3'], "%m/%d/%Y").date()
    iteration4_pdate = datetime.strptime(iterations_prod['24.4'], "%m/%d/%Y").date()
    
    # convert CTE date to mm/dd/yyyy
    iteration1_cdate = datetime.strptime(iterations_cte['24.1'], "%m/%d/%Y").date()
    iteration2_cdate = datetime.strptime(iterations_cte['24.2'], "%m/%d/%Y").date()
    iteration3_cdate = datetime.strptime(iterations_cte['24.3'], "%m/%d/%Y").date()
    iteration4_cdate = datetime.strptime(iterations_cte['24.4'], "%m/%d/%Y").date()
    
    # 1. Loop through file2 list to find employees who are file2 during the release date
    file2s = data2.split("\n")
    i, j, k, l, m, n = 0, 0, 0, 0, 0, 0
    
    # Get names of file2 file
    def file2_func(itr,iter_date,iter_team):
        for file2 in file2s: 
            if(file2 != ''):    #Until end of file
                file2_data = file2.split(',')
                file2_date = datetime.strptime(file2_data[0], "%m/%d/%y").date()  
                if(file2_date == iter_date):   #CTE
                    return file2_data[1]
                    break
                elif(file2_date > iter_date):  #PROD
                    file21 = file2s[itr-1]
                    file2_split = file21.split(',')
                    file2_name = file2_split[1]
                    file2_team = file2_split[2]
                    # Check team whether it is team1, team2 and team3
                    if((file2_team[0:6] == iter_team)or(file2_team[0:5] == iter_team)or(file2_team[0:3] == iter_team)):
                        return file2_name
                        break
            itr=itr+1
    
    # Get names of file1 file
    file1s = data1.split("\n")
    def file1_func(vac_date): #pass iteration date 
        file1_res = []
        for file1 in file1s: 
            if(file1 != ''):  #eof
                file1_data = file1.split(',')
                file1_beg_date = datetime.strptime(file1_data[1], "%m/%d/%y").date() 
                if(file1_data[2][0:8] != '1/1/0001'):  #null value
                    file1_end_date = datetime.strptime(file1_data[2][0:8], "%m/%d/%y").date() 
                    if((file1_beg_date <= vac_date ) and (file1_end_date >= vac_date)): #file1 falls in iteration date 
                        file1_res.append(file1_data[0])

                elif((file1_beg_date == vac_date) and (file1_data[2][0:8] == '1/1/0001')): # only file1 beg date(single date)
                    file1_res.append(file1_data[0])
                    
        return file1_res   


    file2_vac_prod = {}
    
    # combine file2 and file1 list for 24.1 prod
    iter_p1,iter_v1 = [],[]
    iter_p1.append(file2_func(i,iteration1_pdate,team1)) #team1
    iter_p1.append(file2_func(i,iteration1_pdate,team2)) #team2
    iter_v1 = (file1_func(iteration1_pdate))
    iter_p1 = iter_p1 + iter_v1
    file2_vac_prod[24.1] = iter_p1
    
    # combine file2 and file1 list for 24.2 prod
    iter_p1,iter_v1 = [],[]
    iter_p1.append(file2_func(i,iteration2_pdate,team1)) #team1
    iter_p1.append(file2_func(i,iteration2_pdate,team2)) #team2
    iter_v1 = (file1_func(iteration2_pdate))
    iter_p1 = iter_p1 + iter_v1
    file2_vac_prod[24.2] = iter_p1
    
    # combine file2 and file1 list for 24.3 prod
    iter_p1,iter_v1 = [],[]
    iter_p1.append(file2_func(i,iteration3_pdate,team1)) #team1
    iter_p1.append(file2_func(i,iteration3_pdate,team2)) #team2
    iter_v1 = (file1_func(iteration3_pdate))
    iter_p1 = iter_p1 + iter_v1
    file2_vac_prod[24.3] = iter_p1
    
    # combine file2 and file1 list for 24.4 prod
    iter_p1,iter_v1 = [],[]
    iter_p1.append(file2_func(i,iteration4_pdate,team1)) #team1
    iter_p1.append(file2_func(i,iteration4_pdate,team2)) #team2
    iter_v1 = (file1_func(iteration4_pdate))
    iter_p1 = iter_p1 + iter_v1
    file2_vac_prod[24.4] = iter_p1
    
    # print file2 file1 prod list
#   print('file2_vaction_list_prod:')
#   print(file2_vac_prod)
    
    # combine file2 and file1 list for 24.1 cte
    file2_vac_cte = {}
    iter_c1,iter_v1 = [],[]
    iter_c1.append(file2_func(i,iteration1_cdate,team1)) #team1
    iter_c1.append(file2_func(i,iteration1_cdate,team2)) #team2
    iter_v1 = (file1_func(iteration1_cdate))
    iter_c1 = iter_c1 + iter_v1
    file2_vac_cte[24.1] = iter_c1

    # combine file2 and file1 list for 24.2 cte
    iter_c1,iter_v1 = [],[]
    iter_c1.append(file2_func(i,iteration2_cdate,team1)) #team1
    iter_c1.append(file2_func(i,iteration2_cdate,team2)) #team2
    iter_v1 = (file1_func(iteration2_cdate))
    iter_c1 = iter_c1 + iter_v1
    file2_vac_cte[24.2] = iter_c1
    
    # combine file2 and file1 list for 24.3 cte
    iter_c1,iter_v1 = [],[]
    iter_c1.append(file2_func(i,iteration3_cdate,team1)) #team1
    iter_c1.append(file2_func(i,iteration3_cdate,team2)) #team2
    iter_v1 = (file1_func(iteration3_cdate))
    iter_c1 = iter_c1 + iter_v1
    file2_vac_cte[24.3] = iter_c1
    
    # combine file2 and file1 list for 24.4 cte
    iter_c1,iter_v1 = [],[]
    iter_c1.append(file2_func(i,iteration4_cdate,team1)) #team1
    iter_c1.append(file2_func(i,iteration4_cdate,team2)) #team2
    iter_v1 = (file1_func(iteration4_cdate))
    iter_c1 = iter_c1 + iter_v1
    file2_vac_cte[24.4] = iter_c1


    team3_iter_p = []     #team3 - prod
    team3_iter_c = []     #team3 - cte
    
    #combine file2 and file1 list for prod team3
    team3_iter_p.append(file2_func(i,iteration1_pdate,team3))
    team3_iter_p.append(file2_func(i,iteration2_pdate,team3))
    team3_iter_p.append(file2_func(i,iteration3_pdate,team3))
    team3_iter_p.append(file2_func(i,iteration4_pdate,team3))
    team3_file2_prod = dict(zip(iterations,team3_iter_p))
    
    #combine file2 and file1 list for CTE team3
    team3_iter_c.append(file2_func(i,iteration1_cdate,team3))
    team3_iter_c.append(file2_func(i,iteration2_cdate,team3))
    team3_iter_c.append(file2_func(i,iteration3_cdate,team3))
    team3_iter_c.append(file2_func(i,iteration4_cdate,team3))
    team3_file2_cte = dict(zip(iterations,team3_iter_c))

    # remove file2 file1 list from employee list 
    Employees = data3.split("\n")
    def employee_func(itr,array_file2_vac,team):
        employee_res = []
        i,j = 0,0
        for employee in Employees: 
            if(employee != ''):
                employee_data = employee.split(',')
                l = len(array_file2_vac[itr])
                k=0
                for i in range(l):
                    if(array_file2_vac[itr][i] == employee_data[0]):
                        k=1
                        break
                if((k == 0) and ((team == employee_data[1][0:6]) or (team == employee_data[1][0:5]))):
                    employee_res.append(employee_data[0])
            j=j+1
        return employee_res

    # team1 employee prod 
    emp_onl_prod_241 = employee_func(24.1,file2_vac_prod,team1)
    emp_onl_prod_242 = employee_func(24.2,file2_vac_prod,team1)
    emp_onl_prod_243 = employee_func(24.3,file2_vac_prod,team1)
    emp_onl_prod_244 = employee_func(24.4,file2_vac_prod,team1)
    
    # team1 employee cte
    emp_onl_cte_241 = employee_func(24.1,file2_vac_cte,team1)
    emp_onl_cte_242 = employee_func(24.2,file2_vac_cte,team1)
    emp_onl_cte_243 = employee_func(24.3,file2_vac_cte,team1)
    emp_onl_cte_244 = employee_func(24.4,file2_vac_cte,team1)
    
    # team2 employee prod 
    emp_bat_prod_241 = employee_func(24.1,file2_vac_prod,team2)
    emp_bat_prod_242 = employee_func(24.2,file2_vac_prod,team2)
    emp_bat_prod_243 = employee_func(24.3,file2_vac_prod,team2)
    emp_bat_prod_244 = employee_func(24.4,file2_vac_prod,team2)
    
    # team2 employee cte 
    emp_bat_cte_241 = employee_func(24.1,file2_vac_cte,team2)
    emp_bat_cte_242 = employee_func(24.2,file2_vac_cte,team2)
    emp_bat_cte_243 = employee_func(24.3,file2_vac_cte,team2)
    emp_bat_cte_244 = employee_func(24.4,file2_vac_cte,team2)
    
    # fetch random team1 data for prod 
    random_onl_pdata1 = random.sample(emp_onl_prod_241,8)
    random_onl_pdata2 = random.sample(emp_onl_prod_242,8)
    random_onl_pdata3 = random.sample(emp_onl_prod_243,8)
    random_onl_pdata4 = random.sample(emp_onl_prod_244,8)
    # fetch random team1 data for cte
    random_onl_cdata1 = random.sample(emp_onl_cte_241,4)
    random_onl_cdata2 = random.sample(emp_onl_cte_242,4)
    random_onl_cdata3 = random.sample(emp_onl_cte_243,4)
    random_onl_cdata4 = random.sample(emp_onl_cte_244,4)
    # fetch random team2 data for prod
    random_bat_pdata1 = random.sample(emp_bat_prod_241,6)
    random_bat_pdata2 = random.sample(emp_bat_prod_242,6)
    random_bat_pdata3 = random.sample(emp_bat_prod_243,6)
    random_bat_pdata4 = random.sample(emp_bat_prod_244,6)
    # fetch random team2 data for cte
    random_bat_cdata1 = random.sample(emp_bat_cte_241,4)
    random_bat_cdata2 = random.sample(emp_bat_cte_242,4)
    random_bat_cdata3 = random.sample(emp_bat_cte_243,4)
    random_bat_cdata4 = random.sample(emp_bat_cte_244,4)
    
    
    # write random datas to dyanamodb 
    def write_func(random_data,itr_date,team_n):
        i=0
        l = len(random_data)
        for i in range(l):
            try:  
#               print("Table accessed")
                table.put_item(
                    Item = {
                        "Name"        : random_data[i],
                        "date"        : itr_date,
                        "team"        : team_n
                    }
                )
            except Exception as e:
               print("table not accessed")
    
    ##---- write datas to dyanamodb 
    write_func(random_onl_pdata1,iterations_prod['24.1'],team1)         # team1
    write_func(random_bat_pdata1,iterations_prod['24.1'],team2)         # team2
    write_func(random_onl_cdata1,iterations_cte['24.1'],team1)          # team1
    write_func(random_bat_cdata1,iterations_cte['24.1'],team2)          # team2
    write_func([team3_file2_prod['24.1']],iterations_prod['24.1'],team3) # team3
    write_func([team3_file2_cte['24.1']],iterations_cte['24.1'],team3)   # team3
    print("roster data successfully updates in Dynamo DB")
    
    
