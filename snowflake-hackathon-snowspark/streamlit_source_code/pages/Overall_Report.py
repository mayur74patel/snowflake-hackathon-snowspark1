import streamlit as st
from htbuilder import div,h2,big,styles
from htbuilder.units import rem
import snowflake.connector as sc
import pandas as pd
import altair as alt
import plotly.express as px
from io import StringIO


st.set_page_config(page_title='Hackathon',page_icon=':snowflake:')
st.markdown('''<style> footer {visibility: hidden;}</style>''',unsafe_allow_html=True)

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
        # st.write(Exception)
        # st.write('INSIDE EXCEPT ',acct,user1,pwd1,wh,db) 

# login details
if 'connect' not in st.session_state:
    st.markdown('<style>.block-container{border-style: solid;border-width:3px;margin-top:50px;border-image: linear-gradient(120deg, #c71585 25%, grey 25%, #4169E1 50%,red 50%, #191970 75%, teal 75%) 5;}\
    .block-container.css-128j0gw{visibility: hidden;}p{font-weight:1000;color:#000000;} .css-1offfwp{font-color:black}\
     </style>',unsafe_allow_html=True)
    st.markdown("""
    <h2 style='color:#2F4F4F;font-family:algerian;margin-bottom:1px;text-align: center;'>
    <img src='data:image/png;base64,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' style='width:60px;height:70px'/>  
    Snowflake  Login  <img src='data:image/png;base64,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' style='width:40px;height:40px'/>
    </h2><br>""", unsafe_allow_html=True)

    st.session_state['login']=st.radio('',['Login Config File','Enter Credentials'],horizontal=True,label_visibility="hidden")
    
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
            uploaded_file = st.file_uploader("",label_visibility='hidden')
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

else:

    if st.session_state['connect']==True:

        st.markdown("""<html>
        <h4 style='color:#191970;font-family:algerian;'>
        Summary Report <img src='https://cdn-icons-png.flaticon.com/128/4646/4646968.png' style='width:40px;height:40px'/>:</h4><br>
        """, unsafe_allow_html=True)

        def summary(title, value, color1):
            st.markdown(
                div(
                    style=styles(
                        text_align="center",
                        color=color1,
                        border_color="#008b8b",
                        border_style="groove",
                        margin_bottom="15px",
                        border_radius="13px",
                        padding=(rem(0.7), 0, rem(0), 0)
                    )
                )(
                    h2(style=styles(font_size=rem(1), font_weight=600, padding=0))(title),
                    big(style=styles(font_size=rem(3), font_weight=800, line_height=1))(
                        value
                    ),
                ),
                unsafe_allow_html=True,
            )

        col1,col2,col3=st.columns(3)

        with col1:
            query="select to_decimal(sum(credits_used),15,2) total_credits_spent from snowflake.account_usage.warehouse_metering_history"
            res=st.session_state['sf_con'].execute(query)
            summary("Total Credits Spent on Processing",res.fetchall()[0][0],"#008b8b")
        with col2:
            query="select count(1) total_queries from snowflake.account_usage.query_history"
            res=st.session_state['sf_con'].execute(query)
            summary("Total Number of Queries in Snowflake",res.fetchall()[0][0],"#FFCA33")
        with col3:
            query="select to_decimal(avg(total_elapsed_time)/1000,10,3) avg_query_execution from snowflake.account_usage.query_history"
            res=st.session_state['sf_con'].execute(query)
            summary("Average Query Execution Time(s)",res.fetchall()[0][0],"#74FF33")

        with col1:
            query="select to_decimal(sum(case when IS_SUCCESS='NO' then 1 else 0 end)/count(1)*100,15,2) as failed_login \
                from snowflake.account_usage.login_history"
            res=st.session_state['sf_con'].execute(query)
            summary("Percentage of Failed Logins",res.fetchall()[0][0],"#dc143c")
        with col2:
            query="select count(1) from snowflake.account_usage.databases where deleted is null"
            res=st.session_state['sf_con'].execute(query)
            summary("Current Number of Databases",res.fetchall()[0][0],"#8033FF")
        with col3:
            query="select count(1) from snowflake.account_usage.schemata where deleted is null"
            res=st.session_state['sf_con'].execute(query)
            summary("Current Number of Schemas",res.fetchall()[0][0],"#000080")

        with col1:
            query="select count(1) from snowflake.account_usage.tables where deleted is null"
            res=st.session_state['sf_con'].execute(query)
            summary("Current Number of Tables",res.fetchall()[0][0],"#1E90FF")
        with col2:
            query="select count(1) from snowflake.account_usage.sequences where deleted is null"
            res=st.session_state['sf_con'].execute(query)
            summary("Current Number of Sequences",res.fetchall()[0][0],"#16A085")
        with col3:
            query="select to_number(sum((storage_bytes+stage_bytes+failsafe_bytes))/(1024*1024),10,2) total_bytes from snowflake.account_usage.storage_usage"
            res=st.session_state['sf_con'].execute(query)
            summary("Total Storage data(MB)",res.fetchall()[0][0],"#87467E")


        query="select warehouse_name,to_char(start_time,'YYYY-MM') as month,sum(credits_used) credits_used\
                from snowflake.account_usage.warehouse_metering_history\
                group by warehouse_name,to_char(start_time,'YYYY-MM')"
        res_1=st.session_state['sf_con'].execute(query)
        arr=res_1.fetchall()
        ls1=[]
        ls2=[]
        ls3=[]

        for i in range(0,len(arr)):
            for j in range(0,len(arr[i])):
                if j==0:
                    ls1.append(arr[i][j])
                elif j==1:
                    ls2.append(arr[i][j])
                else:
                    ls3.append(float(arr[i][j]))
        x_axis,y_axis,cnt=ls2,ls1,ls3
        df=pd.DataFrame({
        'Date': x_axis,
        'WH': y_axis,
        'Count':cnt
        })
        bar_chart=alt.Chart(df).mark_bar().encode(
            x="month(Date):O",
            y="Count",
            color="WH:N"
        )
        st.markdown("""<br><h5 style='color:#A52A2A;font-family:algerian;'>Credits burned by warehouse 
        <img src='https://cdn-icons-png.flaticon.com/128/740/740117.png' style='width:30px;height:30px'/>:</h5>
        """, unsafe_allow_html=True)
        st.altair_chart(bar_chart,use_container_width=True)

        query="""select warehouse_name,sum(credits_used) credits_used
                from snowflake.account_usage.warehouse_metering_history
                group by warehouse_name ORDER BY 2 DESC LIMIT 7"""
        res_1=st.session_state['sf_con'].execute(query)
        arr=res_1.fetchall()
        ls1=[]
        ls2=[]
        for i in range(0,len(arr)):
            for j in range(0,len(arr[i])):
                if j==0:
                    ls1.append(arr[i][j])
                else:
                    ls2.append(float(arr[i][j]))
        df=pd.DataFrame({
        'warehouse':ls1,
        'credit':ls2
        })
        st.markdown("""<h5 style='color:#191970;font-family:algerian;'>Credits burned by warehouse  
        <img src='https://cdn-icons-png.flaticon.com/128/3372/3372637.png' style='width:30px;height:30px'/>:</h5>
        """, unsafe_allow_html=True)
        fig=px.pie(df,values='credit',names='warehouse',hover_name='warehouse')
        st.write(fig)

        query="""select user_name, count(1) count, 
            sum(total_elapsed_time/1000 * 
            case warehouse_size
            when 'X-Small' then 1/60/60
            when 'Small'   then 2/60/60
            when 'Medium'  then 4/60/60
            when 'Large'   then 8/60/60
            when 'X-Large' then 16/60/60
            when '2X-Large' then 32/60/60
            when '3X-Large' then 64/60/60
            when '4X-Large' then 128/60/60
            else 0
            end) as estimated_credits
        from snowflake.account_usage.query_history
        group by user_name
        order by 3 desc
        limit 10"""
        res_1=st.session_state['sf_con'].execute(query)
        arr=res_1.fetchall()
        ls1=[]
        ls2=[]
        ls3=[]

        for i in range(0,len(arr)):
            for j in range(0,len(arr[i])):
                if j==0:
                    ls1.append(arr[i][j])
                elif j==1:
                    ls2.append(arr[i][j])
                else:
                    ls3.append(float(arr[i][j]))
        username,count,credits=ls1,ls2,ls3
        df=pd.DataFrame({
        'username':username,
        'credits':credits
        })
        bar_chart=alt.Chart(df).mark_bar().encode(
            y=alt.Y("username:N",sort="-x"),
            x="credits:Q"
        )
        st.markdown("""<h5 style='color:#A52A2A;font-family:algerian;'>Most costly users  
        <img src='https://cdn-icons-png.flaticon.com/512/4221/4221628.png' style='width:30px;height:30px'/>:</h5>
        """, unsafe_allow_html=True)
        st.altair_chart(bar_chart,use_container_width=True)


        query="""select user_name,query_text,query_id,total_elapsed_time /1000 time
        from snowflake.account_usage.query_history
        order by 4 desc
        limit 10;"""
        res_1=st.session_state['sf_con'].execute(query)
        arr=res_1.fetchall()
        ls1=[]
        ls2=[]
        ls3=[]
        ls4=[]

        for i in range(0,len(arr)):
            for j in range(0,len(arr[i])):
                if j==0:
                    ls1.append(arr[i][j])
                elif j==1:
                    ls2.append(arr[i][j])
                elif j==2:
                    ls3.append(arr[i][j])
                else:
                    ls4.append(float(arr[i][j]))
        username,query_text,query_id,time=ls1,ls2,ls3,ls4
        df=pd.DataFrame({
        'query_text':query_text,
        'time':time,
        'username':username
        })
        bar_chart=alt.Chart(df).mark_bar().encode(
            y="query_text:O",
            x="time:Q",
            color="username:N"
        )
        st.markdown("""<h5 style='color:#191970;font-family:algerian;'>Longest running queries  
        <img src='https://cdn-icons-png.flaticon.com/512/4149/4149727.png' style='width:30px;height:30px'/>:</h5>
        """, unsafe_allow_html=True)
        st.altair_chart(bar_chart,use_container_width=True)


        query="""select to_char(start_time,'HH24') as hour, sum(credits_used) credits
        from snowflake.account_usage.warehouse_metering_history wmh 
        group by to_char(start_time,'HH24') 
        order by 1;"""
        res_1=st.session_state['sf_con'].execute(query)
        arr=res_1.fetchall()
        ls1=[]
        ls2=[]

        for i in range(0,len(arr)):
            for j in range(0,len(arr[i])):
                if j==0:
                    ls1.append(arr[i][j])
                else:
                    ls2.append(float(arr[i][j]))
        hour,credits=ls1,ls2
        df=pd.DataFrame({
        'hour':hour,
        'credits':credits
        })
        bar_chart=alt.Chart(df).mark_bar().encode(
            y="credits:Q",
            x="hour:O"
        )
        st.markdown("""<h5 style='color:#A52A2A;font-family:algerian;'>Credits per hour distribution 
        <img src='https://cdn-icons-png.flaticon.com/512/404/404621.png' style='width:30px;height:30px'/>:</h5>
        """, unsafe_allow_html=True)
        st.altair_chart(bar_chart,use_container_width=True)

        query="""select distinct * from (select usage_date
        ,      storage_bytes /1073741824 as data
        ,'storage' type
        from snowflake.account_usage.storage_usage
        union all
        select usage_date
        ,      stage_bytes /1073741824 as data
        ,'stage' type
        from snowflake.account_usage.storage_usage
        union all
        select usage_date
        ,      failsafe_bytes /1073741824 as data
        ,'failsafe_bytes' type
        from snowflake.account_usage.storage_usage) order by 1 
        """
        res_1=st.session_state['sf_con'].execute(query)
        arr=res_1.fetchall()
        ls1=[]
        ls2=[]
        ls3=[]
        for i in range(0,len(arr)):
            for j in range(0,len(arr[i])):
                if j==0:
                    ls1.append(str(arr[i][j]))
                elif j==1:
                    ls2.append(float(arr[i][j]))
                else:
                    ls3.append(arr[i][j])
        usage_date,data,type=ls1,ls2,ls3
        df=pd.DataFrame({
        'usage_date':usage_date,
        'data':data,
        'type':type
        })
        line_chart=alt.Chart(df).mark_line().encode(
            alt.X("usage_date",title="Date"),
            alt.Y("data",title="storage data",scale=alt.Scale(zero=False)),
            color='type'
        )
        st.markdown("""<h5 style='color:#191970;font-family:algerian;'>Storage over time 
        <img src='https://cdn-icons-png.flaticon.com/128/1896/1896543.png' style='width:30px;height:30px'/>:</h5>
        """, unsafe_allow_html=True)
        st.altair_chart(line_chart,use_container_width=True)
    
    else:
        st.markdown('<style>.block-container{border-style: solid;border-width:3px;margin-top:50px;border-image: linear-gradient(120deg, #c71585 25%, grey 25%, #4169E1 50%,red 50%, #191970 75%, teal 75%) 5;}\
    .block-container.css-128j0gw{visibility: hidden;}p{font-weight:1000;color:#000000;} .css-1offfwp{font-color:black}\
     div.st-ck{}</style>',unsafe_allow_html=True)
        st.write('''<b><h2 style='font-family:Georgia;text-align:center;color:#b22222'>Invalid Credentials...🔐</h2></b>''',unsafe_allow_html=True)   
        st.session_state['login']=st.radio('',['Choose Config File','Enter Details'],label_visibility='hidden',horizontal=True)
    
        _,col,_=st.columns([1,17,9])

        if st.session_state['login']=='Enter Details':
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
                uploaded_file = st.file_uploader("",label_visibility='hidden')
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


