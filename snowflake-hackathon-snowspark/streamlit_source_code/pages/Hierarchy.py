import streamlit as st
from streamlit_echarts import st_echarts
import snowflake.connector as sc
from io import StringIO


st.set_page_config(page_title='Hackathon',page_icon=':snowflake:')
# st.markdown('<style>.block-container{border-style: solid;border-width:3px;margin-top:50px;border-image: linear-gradient(120deg, #c71585 25%, grey 25%, #4169E1 50%,red 50%, #191970 75%, teal 75%) 5;}\
#     .block-container.css-128j0gw{visibility: hidden;}p{font-weight:1000;color:#000000;} .css-1offfwp{font-color:black}\
#        footer {visibility: hidden;}</style>',unsafe_allow_html=True)

with open('style.css')as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html = True)

# snowflake connection
def snowflake_connection(acct,user1,pwd1,wh,db):
    try:
        conn=sc.connect(account=acct,user=user1,password=pwd1,warehouse=wh,database=db)
        con_obj=conn.cursor()
        st.session_state['sf_con']=con_obj
        st.session_state['connect']=True
        
    except Exception:
        st.session_state['connect']=False
   

if 'connect' not in st.session_state:

    st.markdown('<style>.block-container{border-style: solid;border-width:3px;margin-top:50px;border-image: linear-gradient(120deg, #c71585 25%, grey 25%, #4169E1 50%,red 50%, #191970 75%, teal 75%) 5;}\
    .block-container.css-128j0gw{visibility: hidden;}p{font-weight:1000;color:#000000;} .css-1offfwp{font-color:black}\
       footer {visibility: hidden;}</style>',unsafe_allow_html=True)
    st.markdown("""
    <h2 style='color:#2F4F4F;font-family:algerian;margin-bottom:1px;text-align: center;'>
    <img src='data:image/png;base64,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' style='width:60px;height:70px'/>  
    Snowflake  Login  <img src='data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAOEAAADgCAMAAADCMfHtAAAAYFBMVEX///8ptegAr+b4/f4VsucAruYgs+hlxe1ux+3n9fy34fX3/P7g8vvw+f05uena8PpVwOuv3vSL0fB/ze/C5vfM6vik2vPK6fjU7fmh2fNZwey24fWX1fKs3fR4y+42uOm2uMj2AAANPUlEQVR4nO1d6XbzKgyMKSFp9n370vr93/LazmaDBJIAN/eczN+WmLE9BrQOBsmxPI8XZbkYn5fkIadqSFENOczSTyc5dqXSpqhgtCp3pCHr4jVkQRvyd/heqaIFtZoHh8wWujXCUIb8IfbKFB0YdQoM2Q3tIZr+dveO/bBwMNx7h0yAIeptKX4rd7bVfL89Q5bgEP2uL+rCQNM1C88QaEA15NLbnFlYa3i+6h865IwNec8vKjzZCiU6BCHof+5/hj0oqeaJYN/THcawUO+49G/Q6eorMuQXFG4z5NDr3GlYodNFPxwlNqIw417nTgP6CCsgQ9D3uijeUYgehhoeMfUwNP1OngQPQwWP+PIwRG7Kn+LD0MWH4bvhw9DFh+G74cPQxYfhu+HD0MWHYX/4Xm9+x9vr8cv7X30y/Npdt+PfzT+fGY+OQ6m0NsZorcY+Q2Z/DE+j54wW6xhqDSa6NXOjxrglsy+G36O2YV2bYxzBX2sSRm3+luHXxnYc4DOi4OJOW+vJHzJca2BGETadMThrtQCtgz0w3JfgRbT4Kf5DZgDLMTvD7wv2/0qqRXzORrkm0NwMHQG25yMjePXZB7Wx5ZiX4Q8gwNZ//4gY4rfsNu1Fd3XMyXC/8PErhObVk+f6DSw55mNYrYCBqfidlBgOgdtWWHLMxdBdAaF/R5YwL7bh3+3IMRNDaAUE/h1z/vgwojBsyTELQ2QFdGC2+RhWr+rvPBNDggBjGI6JDB9yTM5wShHg498l2xrvcmhfwOySM6QJ8PHvkgURd0GDJFa+P/IZFive5SUhON7ru/C9UBKGPMiMHnQhhpCdofB0MUs2gewMlTCI6pclBd8EMjMUrfcNvOJiIDPDiPii70QPMS9DU0zFDCuKSZ5iVoa6jIpknKKWAw5yMlS/Mfxq7Ir4VzUfQ136A3RpOKtYjrkYaqH1wsH0l74F7pGhURu/J4WD2SpqMlkYqlEav8wDUXLMwFAvIv0VAM7yVzU5Q63jfU4A5HJMzDCpALtYCuWYlmFqAXYhk2NKhnqRYgX0QSLHdAwzCbALgRxTMfT4Z9OCLcdEDPMKsAumHJMwzC/ALq6cVzUBw2RbUDrmDDlGM8y4AvpAl2Mswz4F2MXO0OSIWTNpDFMKcD6bMS0CNDliliISP+4KiJL4Wo+0qrE6cFjOx2GOBjM0hN1bzBVwfl41HPTIyXn82qiH0cloteVwXC5CLxvqoP0JveRqxLvbwxcJy4a67/p5NBBM4sEk4CVS2Hdw7r83umSdAS1nnDYtb82Pk1Bdu8sY8MrR41TwuUaYAvzn3ubh891ZQ7dSrTgOK58cNb6U4Q+RKcATGI4yvD+mJZAS31zjl2NLRuWofGESB2wQU4DI/b0HoaCZnFBslwcTcHUMPAswOJC5AuLe8JtHA0syby5VxMpRhUIIxs5TjBdg67fqVQP/czPDFSer2n5djApP9mwPSSDAF8pwcBdTjqfLy0KuaWqarTpDOFvQ+Si031AztKxBm+OZcdHBclsorav9UXmlTna5KVQzptyw8vAJ4Sj6QPLWM+U4mJ+O+yXPqTdfHo8n3qaYFI5S7Rh9USIv8OTYB0ICfDC8DBYkhlw55kZYgA+sqAzrj8D7VHJgxIOtyAF6BV+ONnYLdUPkkZYTD2bGgw3H+Bklx59nRSgzlAS83kEU4OOpXJnha0ZtpXLsbEG9m1UfvskCvDPcs82yWlhYxdpjCykyBHifbjVoy/W1MA+mN5ztG+mprYSCFZB5m2u9BQwctCGoC1eOwCmJTTGYkgDNtNGUc3vD4MoRvASPIleAnUuMBD5BlhyRe8ihyBZgM8mnkY8Xj/sYTg7WuaIZWdSDoPcMiM9w9PqFrcjfTJQjSrD6BZK7RSLAwrYuHEXe60qOYYeJhyCJokiA9StmZ0nKYrvCTi8vQQJFkQDheQmDSQKr4yb0/vv38/wVsAbqjBPGdqkLfpYPEvRSlArQMyFZMIkZYgY1AkEPRTuRnIbQlksW26UX4AYA+EQb4/4+bAeaE5O6rJmEveEyOYKx1gBBddkCUoAoziUB2MS9VthdBv226wV1CRpTL6BL99kAFEsBQfrxVSJHJ+fBPbMYczOmASkADkX2iYd75mEFk9wn2bUFAgSLx38AKQCWi2QGe4t8/LjnVk4wyf0SnU0SQLB8SWTufm+6FLk5V6TdlQ22HNsRF0dXg2X7GwBQHLZ2Wb6qmBDUShaOArvLcIats4bzDLoEIYrtd4BnPBIZHe5gyVG3vhaujdL+is9tEu2ys3jFXuCycRV5ORv7dlCJzRAIx7D9lu3hnBzkaFv8Eq8PbF+sdeC0GQIJgnYF9zbDC/2a8fFgR7LZ/y8YFqtQZ4IQZozMrnZOvPOWurtF23Epe0sjo/q+tqwvTUvxzpdG2/OY+b40nEz5mNDoA+/42S697hbVsCg6BDurBV7fHYI0vP3IPb20C/2cnF2X6VAEdt/D9t+Zl5YEaPLP+92tN2DkblEE2lp0jaecBbGG4cpxyhLgnUB3XfJRBAgOu9ZhvquBJ0emABs41l2I4u34BgS4OA7FcIiI++tkObIF2Pz8yPmdH4BivYHcEQjK7PAariRn4VtkcNMr4Kegp1hcgNM16BKW2NlMOEaJtwI+odwnWAN4ilARgyEcHiDypoSiDSUCrKM0MPsWRNEBQlBs9/PIUWwtxXf3YHBuFyhBWpg8AGx1lAkwZN/Cai++xntPr7zAiwdAOUpWwKI+XodCbP75bUrD0PGcaWh4crRXx4PM80SJWpx4vWsE+4PA7lc4chRVv6A686HWag+CpO2kUI6tD/yX6GWnG5jRp0gjOKjtfqJX7GmHJ9sp2qM5MW4IRTLBgdCTr+9tmLj7+IIowBDFYGvELiTO4Jv5j29AFwS3AXtRJsFqORPIsbEPsYuWiQIUHYpsggPJ6qi31YeK+QilQaYWRWGfSrYcVZ14xuInNzB3KMobcTJXR72jFWK9I87AfHxF0MY0/6OX/GwYXjlLRWzA/mx0/6FxnKGaE6JhxoxI9sgo75RghNmQY/WZiTPZQV4dV7SMkkcl3TcC0StmRqTlkCvA5W69PvKGzI7r9Y73gSWtjmZLMNsxBbgbNVlaWukx1TW7G9+HKPKQBgQ5VmcouKty6ybwBHgsXy+PQfpEWNgvOkM49vlwNfM6TxZvd1qwBWhbQswwbIi2AosIBsHOFf2ro6mtnP98RZxZAoQsIYCluAvXZsh0l3lXx5sNAW6nXbAFCFtCAj1SgD4vbHcZLkdzM1TDLdG5Z0A0jtobBYydTWn2+QdQOT6c6zvgeMHcgvqccZ4yxp6zaQo5Dp9fLZei3Z7Dj+l26PmkaTxh3ZcayJSj608ybSvJrPtnpgBDQfAKexsQfTynwZOj1ahFL7r7+4NGC58EEPYFoNVbgrsNnvf66/osQaOVceW/25Z1fmfxy8qbo3jD0Qo8hJMbT46DybhoklQ32N5oytxfE30BWBUlyhGIH0wyT5iSTc1CQdoxEZM7e6mVCILujEOqmZGjSHmrYypwwlESVKTrvWIbL2chQVXB3spe3nFmxoPFM6xLVwkyh4Vge8MTVffsS46C/K90FVp5q6MIc4k3PF2VXWbpKgFkeZhJKyXrnHKUFmZPW+06nxzlxfWTVyz3NO2Vg58mlI1hntUxoqp+ns4BieU4iWtUkqX7Q0o5Rna3yNfBI5EcYwSYlWGq1TFKgJkZFkDTXi7msoz43hjWZvmoV/WU4AEWmTtaGR3hew/5pajI3HdN1Ay4wfR/0jvPFFKGjGJ1fmTvDijpJDsgtK0mI3uHx6HsPU32CHvo0il6iMyM8dSdVnm3V9RplZUxXu2gPH+VMHQrKPsgiXTkZYxXu+DUHY9Z5XdEHUzocae3rVP6vtyM2C5Ru1xqPvXjNJqjtzo5titjb/VKgPcvdZbu8dRQS9HHlBR42vKGZ2FIDbXUkrjCQ/iXO6aETAwd7zX875LI0OC22zp/ZmNISYqUta0O8bNsCPkYhuVooBTWMLyvqWsHysmwkqM3OVkJw5c91wdseXkZevNbxY253VJI9x8EDUC5GXrCIobi3AawKCBmxMvOsJIjnEUYE4IOVM1DDbE9MIRrBcRZ+K9WwJrHetcLQ1eOZhhZ7n/ZClH2d7rqiWHTXuzFL0Vbit2oiTLXWhVXX7RRXwwHg/nGPKY0StPZebr/uW7Ok8DN6o9hhdnkurmu9/22feyV4Z/gw9DFh+G74cPQxYfhu+HD0MWH4bvhw9DFh+G7wcNQ0Hn8HRnis8UySnw3RWo9ywnck4Paa9Hs1U5B2rcBXhkXzYXDnbBxNbozYYaqCi3VghfTlQcB5QT2znks0hhBoR8iN4BaULfngTsVsH7SosCDHgD7xc3FMwR+7oEM978DHAdnfCZIsF6wPE4tO4CPTahgEhBaZpzq5m+EmV2oUheh2Z7sjiy6fLdSOF10qsYZRRDUtFNKB++O9TY4jZQ2NbQejmh+vf3qOUSN3607NoT5ZDO+XMabHT2nWjCEjP8A3Jm73L5QJ3YAAAAASUVORK5CYII=' style='width:40px;height:40px'/>
    </h2><br>""", unsafe_allow_html=True)
    st.session_state['login']=st.radio('login',['Login Config File','Enter Credentials'],label_visibility='hidden',horizontal=True)
    
    _,col,_=st.columns([1,17,9])

    if st.session_state['login']=='Enter Credentials':
        with col:
            st.session_state['acct']=st.text_input('Account')
            st.session_state['user1']=st.text_input('Username')
            st.session_state['pwd1']=st.text_input('Password',type='password')
            st.session_state['wh']=st.text_input('Warehouse')
            st.session_state['db']=st.text_input('Database')
            st.markdown(f""" <style> div.stButton > button:first-child {{ background-color: #13aa52;   border: 1px solid #13aa52;   border-radius: 4px;   box-shadow: rgba(0, 0, 0, .1) 0 2px 4px 0;   box-sizing: border-box;   color: #fff;   cursor: pointer;   font-family: "Akzidenz Grotesk BQ Medium", -apple-system, BlinkMacSystemFont, sans-serif;   font-size: 16px;   font-weight: 400;   outline: none;   outline: 0;   padding: 10px 25px;   text-align: center;   transform: translateY(0);   transition: transform 150ms, box-shadow 150ms;   user-select: none;   -webkit-user-select: none;   touch-action: manipulation; }} <style> """ , unsafe_allow_html=True)
            st.button('Connect',on_click=snowflake_connection,args=[st.session_state['acct'],st.session_state['user1'],st.session_state['pwd1'],st.session_state['wh'],st.session_state['db']])
    else:
        with col:
            uploaded_file = st.file_uploader("file upload",label_visibility='hidden')
            if uploaded_file is not None:
                # To read file as bytes:
                bytes_data = uploaded_file.getvalue()

                # To convert to a string based IO:
                stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))

                # To read file as string:
                string_data = stringio.read()
                l1=string_data.split(',')
                l2=[]
                for i in l1:
                    l2.append(i.split('=')[1].replace("'",""))

                snowflake_connection(l2[0],l2[1],l2[2],l2[3],l2[4])
                # st.write(connection(l2[0],l2[1],l2[2],l2[3],l2[4])) 
                st.experimental_rerun()

else:

    if st.session_state['connect']==True:
        st.markdown('''<h2 style='color:#C71585;font-family: Rockwell;text-align: center;'>Role Lineage... <img src="https://cdn-icons-png.flaticon.com/128/3286/3286746.png" style='width:70px;height:50px'/></h2>''',unsafe_allow_html=True)
        query_1='''select distinct GRANTEE_NAME,NAME  from DB_TEST.SC_TEST.STG_GRANTS_TO_ROLE \
            where GRANTED_ON='ROLE' and PRIVILEGE<>'OWNERSHIP' '''
        res_1=st.session_state['sf_con'].execute(query_1)
        test_arr=res_1.fetchall()

        def get_list_dis_role(ls):
            temp=[]
            for i in ls:
                for j in i:
                    if j not in temp:
                        temp.append(j)
            return temp

        def get_ls_cnt_rl(role_name):
            n=role_name
            v_arr=[n]
            index=0

            def get_value(n):
                temp=[]
                for t in test_arr:
                    if n == t[0]:
                        temp.append(t[1])
                return temp
            def call_func(index):
                if (index<len(v_arr) ):
                    #print(v_arr)
                    for i in get_value(v_arr[index]):
                        if i not in v_arr:
                            v_arr.append(i)
                    index=index+1
                    return call_func(index)
                else:
                    return(v_arr)
            return len(call_func(0))

        def fn_get_fl(temp):
            ds=[]
            for role in temp:
                t=get_ls_cnt_rl(role)
                if t<5:
                    ds.append(role) 
            return ds


        def get_hr(role_name):
            query='''TRUNCATE TABLE DB_TEST.SC_TEST.TEMP'''
            res=st.session_state['sf_con'].execute(query)
            query='''call DB_TEST.SC_TEST.sp_load_role('{role_name}')'''.format(role_name=role_name)
            res=st.session_state['sf_con'].execute(query)
            query='''call DB_TEST.SC_TEST.sp_update_temp_role('{role_name}')'''.format(role_name=role_name)
            res=st.session_state['sf_con'].execute(query)

        def get_wh(role_name):
            get_hr(role_name)
            query='''SELECT LABEL1,LABEL2 FROM DB_TEST.SC_TEST.VW_ROLE_HR_BKP_FINAL \
                WHERE GRANTEE_NAME='{role_name}' and label2 like '%WAREHOUSE%' order by rank \
                '''.format(role_name=role_name)
            res=st.session_state['sf_con'].execute(query)
            test=res.fetchall()
            js={}
            ls=[]
            for j in range(len(test)):
                temp_js={}
                temp_ls=[]
                temp_ls_js={}
                temp_js["name"]=test[j][1]
                temp_ls_js["name"]=test[j][0]
                temp_ls_js["value"]=0
                temp_ls.append(temp_ls_js)
                temp_js["children"]=temp_ls
                ls.append(temp_js)
            js["name"]='WAREHOUSE'
            js["children"]=ls
            return js
        def get_json_list(test):
            ls=[]
            for j in range(len(test)):
                temp_js={}
                temp_ls=[]
                temp_ls_js={}
                temp_js["name"]=test[j][1]
                temp_ls_js["name"]=test[j][0]
                temp_ls_js["value"]=0
                temp_ls.append(temp_ls_js)
                temp_js["children"]=temp_ls
                ls.append(temp_js)
            return ls
        def get_sch_json_list(test,sch):
            ls=[]
            js={}
            js["name"]=sch
            temp_ls=[]
            for j in range(len(test)):
                temp_js={}
                temp_js["name"]=test[j][0]
                temp_js["value"]=0
                temp_ls.append(temp_js)
            js["children"]=temp_ls
            return js
        def get_dis_sch_name(role_name,db_name):
            sc_query='''SELECT distinct T2.LABEL1 FROM DB_TEST.SC_TEST.VW_ROLE_HR_BKP_FINAL T1 \
                INNER JOIN DB_TEST.SC_TEST.VW_ROLE_HR_BKP_FINAL T2
                ON T1.LABEL2=T2.LABEL1
                WHERE T1.GRANTEE_NAME='{role_name}' and T1.label1='{db_name}' and T1.rank=4 \
                '''.format(role_name=role_name,db_name=db_name)
            sc_res=st.session_state['sf_con'].execute(sc_query)
            sc_test=sc_res.fetchall()[0]
            return sc_test        
        def get_db_schema(role_name,db_name):
                sch_name=get_dis_sch_name(role_name,db_name)
                test=[]
                for i in range(len(sch_name)):
                    sc_query='''SELECT T2.LABEL2,T2.LABEL1 FROM DB_TEST.SC_TEST.VW_ROLE_HR_BKP_FINAL T1 \
                                INNER JOIN DB_TEST.SC_TEST.VW_ROLE_HR_BKP_FINAL T2
                                ON T1.LABEL2=T2.LABEL1
                                WHERE T1.GRANTEE_NAME='{role_name}' and T1.label1='{db_name}' and T2.LABEL1='{sch_name}' 
                                and T1.rank=4 order by T1.rank \
                                '''.format(role_name=role_name,db_name=db_name,sch_name=sch_name[i])
                    sc_res=st.session_state['sf_con'].execute(sc_query)
                    sc_test=sc_res.fetchall()
                    test.append(get_sch_json_list(sc_test,sch_name[i]))
                return test
        def get_db(role_name):
            get_hr(role_name)   
            query='''SELECT LABEL1,LABEL2 FROM DB_TEST.SC_TEST.VW_ROLE_HR_BKP_FINAL \
                WHERE GRANTEE_NAME='{role_name}' and label2 like '%DATABASE%' order by rank \
                '''.format(role_name=role_name)
            res=st.session_state['sf_con'].execute(query)
            test=res.fetchall()
            js={}
            ls=[]
            for j in range(len(test)):
                temp_js={}
                temp_ls=[]
                temp_ls_js={}
                temp_js["name"]=test[j][1]    #DB_NAME
                temp_ls_js["name"]=test[j][0] #DB_PRIVILEGE
                temp_ls_js["value"]=0
                temp_ls.append(temp_ls_js)
                sc_temp_js={}
                sc_temp_js["name"]='SCHEMA'
                sc_temp_js["children"]=get_db_schema(role_name,test[j][1])
                temp_ls.append(sc_temp_js)
                temp_js["children"]=temp_ls
                ls.append(temp_js)
            js["name"]='DATABASE'
            js["children"]=ls
            return js
        _,col,_=st.columns([1,3,1])
        with col:
            st.session_state['role']=st.selectbox('Select Role',fn_get_fl(get_list_dis_role(test_arr)))

        if st.session_state['role'] is not None:
            data={
            "name":st.session_state['role'],
            "children":[
                                    get_wh(st.session_state['role']),
                                    get_db(st.session_state['role']),
                                    
                        ],
            }

            opts = {
                "tooltip": {"trigger": "item", "triggerOn": "mousemove"},
                "series": [
                    {
                        "type": "tree",
                        "data": [data],
                        "top": "1%",
                        "left": "12%",
                        "bottom": "1%",
                        "right": "23%",
                        "symbolSize": 7,
                        "label": {
                            "position": "left",
                            "verticalAlign": "middle",
                            "align": "right",
                            "fontSize": 11,
                        },
                        "leaves": {
                            "label": {
                                "position": "right",
                                "verticalAlign": "middle",
                                "align": "left",
                            }
                        },
                        "expandAndCollapse": True,
                        "animationDuration": 550,
                        "animationDurationUpdate": 750,
                    }
                ],
            }

            st_echarts(opts, height=800)

    else:

        st.markdown('<style>.block-container{border-style: solid;border-width:3px;margin-top:50px;border-image: linear-gradient(120deg, #c71585 25%, grey 25%, #4169E1 50%,red 50%, #191970 75%, teal 75%) 5;}\
    .block-container.css-128j0gw{visibility: hidden;}p{font-weight:1000;color:#000000;} .css-1offfwp{font-color:black}\
       footer {visibility: hidden;}</style>',unsafe_allow_html=True)
        
        st.write('''<b><h2 style='font-family:Georgia;text-align:center;color:#b22222'>Invalid Credentials...🔐</h2></b>''',unsafe_allow_html=True)   
        st.session_state['login']=st.radio('login',['Login Config File','Enter Credentials'],label_visibility='hidden',horizontal=True)
    
        _,col,_=st.columns([1,17,9])

        if st.session_state['login']=='Enter Credentials':
            with col:
                st.session_state['acct']=st.text_input('Account')
                st.session_state['user1']=st.text_input('Username')
                st.session_state['pwd1']=st.text_input('Password',type='password')
                st.session_state['wh']=st.text_input('Warehouse')
                st.session_state['db']=st.text_input('Database')
                st.markdown(f""" <style> div.stButton > button:first-child {{ background-color: #13aa52;   border: 1px solid #13aa52;   border-radius: 4px;   box-shadow: rgba(0, 0, 0, .1) 0 2px 4px 0;   box-sizing: border-box;   color: #fff;   cursor: pointer;   font-family: "Akzidenz Grotesk BQ Medium", -apple-system, BlinkMacSystemFont, sans-serif;   font-size: 16px;   font-weight: 400;   outline: none;   outline: 0;   padding: 10px 25px;   text-align: center;   transform: translateY(0);   transition: transform 150ms, box-shadow 150ms;   user-select: none;   -webkit-user-select: none;   touch-action: manipulation; }} <style> """ , unsafe_allow_html=True)
                st.button('Connect',on_click=snowflake_connection,args=[st.session_state['acct'],st.session_state['user1'],st.session_state['pwd1'],st.session_state['wh'],st.session_state['db']])
        else:
            with col:
                uploaded_file = st.file_uploader("file upload",label_visibility='hidden')
                if uploaded_file is not None:
                    # To read file as bytes:
                    bytes_data = uploaded_file.getvalue()

                    # To convert to a string based IO:
                    stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))

                    # To read file as string:
                    string_data = stringio.read()
                    l1=string_data.split(',')
                    l2=[]
                    for i in l1:
                        l2.append(i.split('=')[1].replace("'",""))

                    snowflake_connection(l2[0],l2[1],l2[2],l2[3],l2[4])
                    st.experimental_rerun()