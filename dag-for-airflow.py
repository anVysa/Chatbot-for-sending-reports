import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import io
import telegram
import pandahouse as ph
import os
from dotenv import load_dotenv

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import datetime

OWNER = os.getenv('OWNER')
default_args = {
    'owner': OWNER, # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': datetime.timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime.datetime(2024, 6, 27), # Дата начала выполнения DAG
}

schedule_interval = '0 11 * * *' # будет выполняться каждый день в 11 часов

def connect_bd(q):
    load_dotenv()
    
    HOST = os.getenv('HOST')
    DATABASE = os.getenv('DATABASE')
    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    
    connection_ = {'host': HOST,
                   'database': DATABASE,
                   'user': USER,
                   'password':  PASSWORD}

    data = ph.read_clickhouse(q, connection=connection_)
    return data

def new_gone_ret(shema):
    q = """WITH main_table as (SELECT user_id, 
    groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
    arrayJoin(groupUniqArray(toMonday(toDate(time)))) as this_week,
    addWeeks(arrayJoin(weeks_visited), +1) as next_week, 
    addWeeks(arrayJoin(weeks_visited), -1) as prev_week,
    if(has(weeks_visited, next_week) = 1, 'ret', 'gone') as status_next_week, 
    if(has(weeks_visited, prev_week) = 1, 'ret', 'new') as status_this_week 
    FROM {db}."""+shema+"""
    group by user_id)

    select this_week, new_users, retention_users, gone_users from

    (SELECT this_week, count(user_id) as new_users FROM main_table
    group by this_week) t1

    LEFT JOIN

    (SELECT next_week, 
        countIf(user_id, status_next_week = 'ret') as retention_users, 
        -countIf(user_id, status_next_week = 'gone') as gone_users 
    FROM main_table
    group by next_week) t2

    ON t1.this_week = t2.next_week
    ORDER BY this_week"""
    
    return connect_bd(q)

def get_word(data, column):
    dif = int(data.iloc[-1,column]) - int(data.iloc[-2,column])
    word = 'меньше' if dif < 0 else 'больше'
    return word, abs(dif)

@dag(default_args = default_args, schedule_interval = schedule_interval)
def dag_for_feed_and_message_report():
    
    #суточная активность ленты и мессенджера
    @task
    def get_dau_feed_and_mes():
        context = get_current_context()
        y_day = context['ds'] # вчерашний день
        
        q_day_feed_and_mes = """select time, count(user_id) as count_users from 
        (SELECT toStartOfHour(time) as time,
                user_id
        FROM {db}.message_actions
        WHERE toStartOfHour(time) >= toStartOfHour(now()) - INTERVAL 47 HOUR) t1
        join
        (SELECT distinct user_id
        FROM {db}.feed_actions
        WHERE toStartOfHour(time) >= toStartOfHour(now()) - INTERVAL 47 HOUR) t2
        on t1.user_id = t2.user_id
        group by time
        order by time"""
        return connect_bd(q_day_feed_and_mes)
    
    #суточная активность ленты 
    @task
    def dau_feed():
        context = get_current_context()
        y_day = context['ds'] # вчерашний день
        
        q_day_feed = """SELECT toStartOfHour(time) as time, count(user_id) as count_users
        FROM {db}.feed_actions
        WHERE toStartOfHour(time) >= toStartOfHour(now()) - INTERVAL 47 HOUR
        group by time
        order by time"""
        return connect_bd(q_day_feed)
    
    #суточная активность мессенджера
    @task
    def dau_mes():
        context = get_current_context()
        y_day = context['ds'] # вчерашний день
        
        q_day_mes = """SELECT toStartOfHour(time) as time, count(user_id) as count_users
        FROM {db}.message_actions
        WHERE toStartOfHour(time) >= toStartOfHour(now()) - INTERVAL 47 HOUR
        group by time
        order by time"""
        return connect_bd(q_day_mes)
    
    #DAU по городам ленты
    @task
    def feed_city():
        context = get_current_context()
        y_day = context['ds'] # вчерашний день
        
        cities = ['Moscow','Saint Petersburg','Yekaterinburg','Novosibirsk','Rostov']
        q_feed_city = """
        SELECT toDate(time) as date,
               count(distinct user_id) as DAU,
               city
        FROM {db}.feed_actions
        WHERE date <= toDate(\'"""+str(y_day)+"""\')+1 AND date >= toDate(\'"""+str(y_day)+"""\') - 7 AND
        country = 'Russia' AND city in """+str(cities)+"""
        GROUP BY date, city 
        ORDER BY date
        """
        return connect_bd(q_feed_city)
    
    #DAU по городам мессенджера
    @task
    def message_city():
        context = get_current_context()
        y_day = context['ds'] # вчерашний день
        
        cities = ['Moscow','Saint Petersburg','Yekaterinburg','Novosibirsk','Rostov']
        q_message_city = """
        SELECT toDate(time) as date,
               count(distinct user_id) as DAU,
               city
        FROM {db}.message_actions
        WHERE date <= toDate(\'"""+str(y_day)+"""\')+1 AND date >= toDate(\'"""+str(y_day)+"""\') - 7 AND
        country = 'Russia' AND city in """+str(cities)+"""
        GROUP BY date, city 
        ORDER BY date
        """
        return connect_bd(q_message_city)
    
    #метрики ленты новостей (среднее количество лайков, просмотров в день, dau)
    @task
    def feed_metrics():
        context = get_current_context()
        y_day = context['ds'] # вчерашний день
        
        q_feed_metrics = """select date, count(distinct user_id) as DAU, avg(Likes) as Avg_likes, avg(Views) as Avg_views from
        (SELECT toDate(time) as date,
               user_id,
               countIf(action = 'like') as Likes,
               countIf(action = 'view') as Views
        FROM {db}.feed_actions
        WHERE date <= toDate(\'"""+str(y_day)+"""\') AND date >= toDate(\'"""+str(y_day)+"""\') - 27
        GROUP BY date, user_id)
        group by date
        order by date
        """
        return connect_bd(q_feed_metrics)
    
    #кол-во уникальных пользователей месседжера 
    #и вовлеченность пользователей - количество отправленных сообщений/кол-во уникальных пользователей
    @task
    def dau_message():
        context = get_current_context()
        y_day = context['ds'] # вчерашний день
        
        q_dau_message = """SELECT toDate(time) as date,
               count(distinct user_id) as DAU,
               count(time)/DAU as ER 
        FROM {db}.message_actions
        WHERE date <= toDate(\'"""+str(y_day)+"""\') AND date >= toDate(\'"""+str(y_day)+"""\') - 27
        GROUP BY date
        ORDER BY date
        """
        return connect_bd(q_dau_message)
    
    #кол-во уникальных пользователей месседжера и ленты новостей
    @task
    def dau_mes_and_feed():
        context = get_current_context()
        y_day = context['ds'] # вчерашний день
        
        q_dau_mes_and_feed = """
        select date, count(distinct user_id) as DAU from 
        (SELECT toDate(time) as date,
                user_id
        FROM {db}.message_actions
        WHERE date <= toDate(\'"""+str(y_day)+"""\') AND date >= toDate(\'"""+str(y_day)+"""\') - 27) t1
        join
        (SELECT distinct user_id
        FROM {db}.feed_actions
        WHERE toDate(time) <= toDate(\'"""+str(y_day)+"""\') AND toDate(time) >= toDate(\'"""+str(y_day)+"""\') - 27) t2
        on t1.user_id = t2.user_id
        group by date
        order by date"""
        return connect_bd(q_dau_mes_and_feed)
    
    #новые, вернувшиеся и ушедшие пользователи ленты
    @task
    def new_feed():
        return new_gone_ret('feed_actions')
    
    #новые, вернувшиеся и ушедшие пользователи мессенджера
    @task
    def new_message():
        return new_gone_ret('message_actions')
    
    @task
    def get_dashboard(dau_mes,data_feed,data_feed_city,data_message_city, data_dau_feed_and_mes, data_dau_feed, data_dau_mes,data_new_feed,data_new_message,data_dau_mes_and_feed):
        load_dotenv()
     
        TOKEN = os.getenv('TOKEN')  
        CHAT_ID = os.getenv('CHAT_ID') 
        
        metrics = 'Метрики от {0}\n\n\
Лента новостей\nDAU: {1}\n\
Среднее кол-во лайков на пользователя: {2}\n\
Среднее кол-во просмотров на пользователя: {3}\n\
Количество новых пользователей на этой неделе: {4},\
 что на {5} {6} по сравнению с прошлой неделей\n\
\nМессенджер\nDAU: {7}\n\
Вовлеченность пользователей: {8}\n\
Количество новых пользователей на этой неделе: {9},\
 что на {10} {11} по сравнению с прошлой неделей\n\n\
DAU ленты новостей и мессенджера: {12}'\
      .format(data_feed.iloc[-1,0].date(),
              data_feed.iloc[-1,1],
              round(data_feed.iloc[-1,2],1),
              round(data_feed.iloc[-1,3],1),
              data_new_feed.iloc[-1,1], get_word(data_new_feed, 1)[1], get_word(data_new_feed, 1)[0],
              dau_mes.iloc[-1,1],
              round(dau_mes.iloc[-1,2],1),
              data_new_message.iloc[-1,1], get_word(data_new_message, 1)[1], get_word(data_new_message, 1)[0],
              data_dau_mes_and_feed.iloc[-1,1])
        
        url_for_text = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&text={metrics}"     
        requests.get(url_for_text)
        
        fig, ax = plt.subplots(4,3,constrained_layout=True, dpi = 400, figsize = (9,12))

        ax[0,0].set_title('Мессенджер и лента новостей\nСуточная активность', fontsize = 10)
        ax[0,1].set_title('Лента новостей\nСуточная активность', fontsize = 10)
        ax[0,2].set_title('Мессенджер\nСуточная активность', fontsize = 10)
        
        #Суточная активность
        daus_metrics = {'Мессенджер и лента новостей':data_dau_feed_and_mes,'Лента новостей':data_dau_feed,'Мессенджер':data_dau_mes}
        ax[0,0].set_ylabel('Количество пользователей')
        for i, (key, val) in enumerate(daus_metrics.items()):
            ax[0,i].grid()
            ax[0,i].plot(val.iloc[24:,0], val.iloc[24:,1], '-o', markersize = 3, 
                               linewidth = 2, label = 'Последние 24 часа', color = 'teal')
            ax[0,i].plot(val.iloc[24:,0], val.iloc[:24,1], '--', linewidth = 2, 
                             label = 'Прошлые сутки', alpha = 0.7, color = 'teal')
            ax[0,i].tick_params(labelsize = 7)
            ax[0,i].legend(loc = 'upper left', fontsize = 7)
            ax[0,i].tick_params('x',rotation = 45)
        
        
        #DAU и WAU
        ax[1,0].grid()
        ax[1,0].set_ylabel('Количество пользователей')
        ax[1,0].plot(data_dau_mes_and_feed.iloc[:,0], data_dau_mes_and_feed.iloc[:,1], '-o', markersize = 3, 
                               linewidth = 2, color = 'seagreen')
        ax[1,0].set_title('Мессенджер и лента новостей\nDAU', fontsize = 10)
        ax[1,0].tick_params('x',rotation = 45)
        ax[1,0].tick_params(labelsize = 7)
        
        ax[1,1].grid()
        ax[1,1].bar(data_new_feed.this_week, data_new_feed.iloc[:,1], width = 5,
                           label = 'Новые', zorder = 10)
        ax[1,1].bar(data_new_feed.this_week, data_new_feed.iloc[:,2], width = 5,
                           label = 'Остались', zorder = 10)
        ax[1,1].bar(data_new_feed.this_week, data_new_feed.iloc[:,3], width = 5,
                           label = 'Ушли', zorder = 10)
        ax[1,1].set_title('Лента новостей\nДекомпозиция WAU', fontsize = 10)
        ax[1,1].legend(loc = 'lower left', fontsize = 7)
        ax[1,1].tick_params('x',rotation = 45)
        ax[1,1].tick_params(labelsize = 7)
        
        ax[1,2].grid()
        ax[1,2].bar(data_new_message.this_week, data_new_message.iloc[:,1], width = 5,
                           label = 'Новые', zorder = 10)
        ax[1,2].bar(data_new_message.this_week, data_new_message.iloc[:,2], width = 5,
                 label = 'Остались', zorder = 10)
        ax[1,2].bar(data_new_message.this_week, data_new_message.iloc[:,3], width = 5,
                 label = 'Ушли', zorder = 10)
        ax[1,2].set_title('Мессенджер\nДекомпозиция WAU', fontsize = 10)
        ax[1,2].legend(loc = 'lower left', fontsize = 7)
        ax[1,2].tick_params('x',rotation = 45)
        ax[1,2].tick_params(labelsize = 7)
        
        #Активность по топ 5 городам и вовлеченность пользователей мессенджера
        ax[2,2].grid()
        ax[2,0].set_ylabel('Количество пользователей')
        ax[2,2].plot(dau_mes.iloc[:,0], dau_mes.iloc[:,2], '-o', markersize = 3, 
                           linewidth = 2, color = 'salmon')
        ax[2,2].set_title('Мессенджер\nВовлеченность пользователей', fontsize = 10)
        ax[2,2].tick_params('x',rotation = 45)
        ax[2,2].tick_params(labelsize = 7)
        
        ax[2,0].tick_params('x',rotation = 45)
        ax[2,0].set_title('Лента новостей\nDAU топ 5 городов', fontsize = 10)
        ax[2,0].grid()
        cities = ['Moscow','Saint Petersburg','Yekaterinburg','Novosibirsk','Rostov']
        for i, city in enumerate(cities):
            dau = data_feed_city[data_feed_city.city == city].DAU
            t = data_feed_city[data_feed_city.city == city].date
            ax[2,0].bar(t,dau,label = city, zorder = 3)
            ax[2,0].tick_params(labelsize = 7)
            
        ax[2,1].tick_params('x',rotation = 45)
        ax[2,1].set_title('Мессенджер\nDAU топ 5 городов', fontsize = 10)
        ax[2,1].grid()
        
        for i, city in enumerate(cities):
            dau = data_message_city[data_message_city.city == city].DAU
            t = data_message_city[data_message_city.city == city].date
            ax[2,1].bar(t,dau,label = city, zorder = 3)
            ax[2,1].tick_params(labelsize = 7)
        ax[2,1].legend(fontsize = 7, loc = 'upper left')
        
        #Метрики ленты новостей
        ax[3,0].set_ylabel('Количество пользователей')
        ax[3,0].set_title('Лента новостей\nDAU', fontsize = 10)
        ax[3,1].set_title('Лента новостей\nСреднее количество лайков', fontsize = 10)
        ax[3,2].set_title('Лента новостей\nСреднее количество просмотров', fontsize = 10)
        for i, (key, val) in enumerate(data_feed.iloc[:,1:].to_dict('list').items()):
            ax[3,i].grid()
            ax[3,i].plot(data_feed.date[14:], val[14:], '-o', markersize = 3, 
                               linewidth = 2, color = 'indianred', label = 'Текущие 2 недели')
            ax[3,i].plot(data_feed.date[14:], val[:14], '--', linewidth = 2, color = 'indianred', 
                             label = 'Предыдущие', alpha = 0.7)
            ax[3,i].tick_params(labelsize = 7)
            ax[3,i].legend(loc = 'upper left', fontsize = 7)
            ax[3,i].tick_params('x',rotation = 45)

        plot_obj = io.BytesIO() # буфер
        plt.savefig(plot_obj) # сохраняем туда красивую картинку
        plot_obj.seek(0) # переносим курсор в начало
        plot_obj.name = 'Дашборд.png'
        plt.close()
        url = f'https://api.telegram.org/bot{TOKEN}/sendPhoto'
        files = {'photo': plot_obj}
        data = {'chat_id': CHAT_ID}
        response = requests.post(url, files=files, data=data)
        
    data_dau_message = dau_message()
    data_feed = feed_metrics()
        
    data_feed_city = feed_city()
    data_message_city = message_city()

    data_dau_feed_and_mes = get_dau_feed_and_mes()
    data_dau_feed = dau_feed()
    data_dau_mes = dau_mes()
    
    data_new_feed = new_feed()
    data_new_message = new_message()
    
    data_dau_mes_and_feed = dau_mes_and_feed()
    
    get_dashboard(data_dau_message,data_feed,data_feed_city,data_message_city,
                  data_dau_feed_and_mes, data_dau_feed, data_dau_mes,data_new_feed,data_new_message,data_dau_mes_and_feed)
    
dag_for_feed_and_message_report = dag_for_feed_and_message_report()