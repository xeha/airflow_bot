from datetime import datetime, timedelta, date
from time import sleep
import pandas as pd
from io import StringIO
import requests
import seaborn as sns
import telegram
import numpy as np
import matplotlib.pyplot as plt
import io

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable
import pandahouse



class Getch:
    def __init__(self, query, db='simulator'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)
# Для телеги
my_token = 'тут_нужно_заменить_на_свой_токен' # тут нужно заменить на токен вашего бота
bot = telegram.Bot(token=my_token) # получаем доступ
chat_id = -1001573194187 

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'k.agrova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 20)
}


# Интервал запуска DAG
schedule_interval = '0 11 * * *'

# Функция для CH
def execute_query(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_agrova_airflow_3():
    @task()
    # Извлекаем таблицу summary за yesterday
    def extract_summary_yesterday():
        query_yesterday = """
                            SELECT *
                            FROM
                              (
                              SELECT *,
                                toDecimal64(total_likes/total_views,2) AS CTR,
                                toDecimal64(total_likes/users_feed,2) AS avg_likes_b_user,
                                toDecimal64(total_views/users_feed,2) As avg_views_b_user
                              FROM
                                (SELECT 
                                  event_datehour,
                                  uniqExact(user_id) AS users_feed,
                                  SUM(likes) AS total_likes,
                                  SUM(views) AS total_views
                                FROM  
                                  (
                                  SELECT 
                                      toStartOfHour(time) AS event_datehour,
                                      user_id,
                                      countIf(user_id, action='like') AS likes,
                                      countIf(user_id, action='view') AS views
                                  FROM simulator_20230320.feed_actions
                                  WHERE toDate(time) = yesterday()
                                  GROUP BY event_datehour, user_id
                                  )
                                GROUP BY event_datehour
                                )
                              ) feed_t
                            JOIN 
                              (
                              SELECT *,
                                toDecimal64(total_msg_send/users_messanger,2) AS avg_msg_send,
                                toDecimal64(total_users_send/users_messanger,2) AS avg_recievers
                              FROM
                                (SELECT 
                                  event_datehour,
                                  uniqExact(user_id) AS users_messanger,
                                  SUM(messages_send) AS total_msg_send,
                                  SUM(users_send) AS total_users_send
                                FROM
                                  (SELECT  
                                    user_id,
                                    toStartOfHour(time) event_datehour,
                                    count(reciever_id) messages_send,
                                    count(distinct reciever_id) users_send
                                  FROM simulator_20230320.message_actions
                                  WHERE toDate(time) = yesterday()
                                  GROUP BY user_id, event_datehour
                                  )
                                GROUP BY event_datehour
                                )
                              ) message_t
                            ON feed_t.event_datehour = message_t.event_datehour
                            format TSVWithNames
                                                """
        df_yesterday = execute_query(query_yesterday)
        df_yesterday['event_datehour'] = df_yesterday['event_datehour'].apply(lambda x : pd.to_datetime(str(x)))
        df_yesterday['time'] = df_yesterday['event_datehour'].dt.time
        df_yesterday['hour'] = df_yesterday['event_datehour'].dt.hour
        return df_yesterday
    
    @task()
    # Извлекаем таблицу summary за week ago
    def extract_summary_week_ago():
        query_week = """
            SELECT *
            FROM
              (
              SELECT *,
                toDecimal64(total_likes/total_views,2) AS CTR,
                toDecimal64(total_likes/users_feed,2) AS avg_likes_b_user,
                toDecimal64(total_views/users_feed,2) As avg_views_b_user
              FROM
                (SELECT 
                  event_datehour,
                  uniqExact(user_id) AS users_feed,
                  SUM(likes) AS total_likes,
                  SUM(views) AS total_views
                FROM  
                  (
                  SELECT 
                      toStartOfHour(time) AS event_datehour,
                      user_id,
                      countIf(user_id, action='like') AS likes,
                      countIf(user_id, action='view') AS views
                  FROM simulator_20230320.feed_actions
                  WHERE toDate(time) = toDate(now()) + INTERVAL -8 DAY 
                  GROUP BY event_datehour, user_id
                  )
                GROUP BY event_datehour
                )
              ) feed_t
            -- -- feed table
            JOIN 
            -- message table
              (
              SELECT *,
                toDecimal64(total_msg_send/users_messanger,2) AS avg_msg_send,
                toDecimal64(total_users_send/users_messanger,2) AS avg_recievers
              FROM
                (SELECT 
                  event_datehour,
                  uniqExact(user_id) AS users_messanger,
                  SUM(messages_send) AS total_msg_send,
                  SUM(users_send) AS total_users_send
                FROM
                  (SELECT  
                    user_id,
                    toStartOfHour(time) event_datehour,
                    count(reciever_id) messages_send,
                    count(distinct reciever_id) users_send
                  FROM simulator_20230320.message_actions
                  WHERE toDate(time) = toDate(now()) + INTERVAL -8 DAY 
                  GROUP BY user_id, event_datehour
                  )
                GROUP BY event_datehour
                )
              ) message_t
            -- message table 
            ON feed_t.event_datehour = message_t.event_datehour
                    format TSVWithNames
                 """
        df_week_ago = execute_query(query_week)
        df_week_ago['event_datehour'] = df_week_ago['event_datehour'].apply(lambda x : pd.to_datetime(str(x)))
        df_week_ago['time'] = df_week_ago['event_datehour'].dt.time
        df_week_ago['hour'] = df_week_ago['event_datehour'].dt.hour
        return df_week_ago 
    
    @task()
    # Извлекаем таблицу summary за month ago
    def extract_summary_month_ago():
        query_month = """
                SELECT *
                FROM
                (
                SELECT *,
                    toDecimal64(total_likes/total_views,2) AS CTR,
                    toDecimal64(total_likes/users_feed,2) AS avg_likes_b_user,
                    toDecimal64(total_views/users_feed,2) As avg_views_b_user
                FROM
                    (SELECT 
                    event_datehour,
                    uniqExact(user_id) AS users_feed,
                    SUM(likes) AS total_likes,
                    SUM(views) AS total_views
                    FROM  
                    (
                    SELECT 
                        toStartOfHour(time) AS event_datehour,
                        user_id,
                        countIf(user_id, action='like') AS likes,
                        countIf(user_id, action='view') AS views
                    FROM simulator_20230320.feed_actions
                    WHERE toDate(time) = toDate(yesterday()) + INTERVAL -1 MONTH 
                    GROUP BY event_datehour, user_id
                    )
                    GROUP BY event_datehour
                    )
                ) feed_t
                -- -- feed table
                JOIN 
                -- message table
                (
                SELECT *,
                    toDecimal64(total_msg_send/users_messanger,2) AS avg_msg_send,
                    toDecimal64(total_users_send/users_messanger,2) AS avg_recievers
                FROM
                    (SELECT 
                    event_datehour,
                    uniqExact(user_id) AS users_messanger,
                    SUM(messages_send) AS total_msg_send,
                    SUM(users_send) AS total_users_send
                    FROM
                    (SELECT  
                        user_id,
                        toStartOfHour(time) event_datehour,
                        count(reciever_id) messages_send,
                        count(distinct reciever_id) users_send
                    FROM simulator_20230320.message_actions
                    WHERE toDate(time) = toDate(yesterday()) + INTERVAL -1 MONTH 
                    GROUP BY user_id, event_datehour
                    )
                    GROUP BY event_datehour
                    )
                ) message_t
                -- message table 
                ON feed_t.event_datehour = message_t.event_datehour
                        format TSVWithNames
                    """
        df_month_ago = execute_query(query_month)
        df_month_ago['event_datehour'] = df_month_ago['event_datehour'].apply(lambda x : pd.to_datetime(str(x)))
        df_month_ago['time'] = df_month_ago['event_datehour'].dt.time
        df_month_ago['hour'] = df_month_ago['event_datehour'].dt.hour
        return df_month_ago 
    @task()
    # Собираем когорту для мессенджера
    def extract_cohort_messager():
        query_cohort_messager = """
                            SELECT 
                                start_date,
                                day,
                                COUNT(id) AS users,
                                date_diff(day, start_date, day) AS period
                            FROM
                            (
                            SELECT 
                                DISTINCT id,
                                start_date,
                                day
                            FROM
                            -- start date
                            (
                            SELECT 
                                id,
                                start_date
                            FROM
                                (
                                SELECT  user_id AS id,
                                        MIN(toDate(time)) AS start_date
                                FROM simulator_20230320.message_actions
                                GROUP BY user_id
                                ) users_start_t
                                FULL JOIN
                                (
                                SELECT  reciever_id  AS id,
                                        min(toDate(time)) AS start_date
                                FROM simulator_20230320.message_actions
                                group by reciever_id
                                ) received_start_t
                                ON users_start_t.id = received_start_t.id
                            )  start_t
                            -- start_date
                            JOIN
                                (
                                SELECT 
                                users_t.id,
                                users_t.event_date AS day
                                FROM
                                (
                                SELECT  user_id AS id,
                                        toDate(time) event_date
                                FROM simulator_20230320.message_actions
                                GROUP BY user_id, event_date
                                ) users_t
                                FULL JOIN
                                (
                                SELECT  reciever_id  AS id,
                                        toDate(time) event_date
                                FROM simulator_20230320.message_actions
                                group by reciever_id, event_date
                                ) received_t
                                ON users_t.id = received_t.id
                                ) t
                            ON t.id = start_t.id
                            HAVING start_date> toDate(yesterday()) + INTERVAL -7 DAY  AND
                                        start_date<= toDate(yesterday())
                            )
                            GROUP BY start_date, day
                            ORDER BY start_date, day
                                                format TSVWithNames
                    """
        df_coh_messager = execute_query(query_cohort_messager)
        df_coh_messager['start_date'] = df_coh_messager['start_date'].apply(lambda x : pd.to_datetime(str(x))).dt.date
        df_coh_messager['day'] = df_coh_messager['day'].apply(lambda x : pd.to_datetime(str(x))).dt.date
        return df_coh_messager
    @task()
    # Собираем когорту для ленты новостей
    def extract_cohort_feed():
        query_cohort_feed = """
                            SELECT start_day,
                                   day,
                                   count(user_id) AS users,
                                   date_diff(day, start_day, day) AS period
                            FROM
                              (
                              SELECT *
                               FROM
                                 (
                                 SELECT 
                                    user_id,
                                    min(toDate(time)) AS start_day
                                  FROM simulator_20230320.feed_actions
                                  GROUP BY user_id
                                  ) t1
                               JOIN
                                 (
                                 SELECT 
                                    DISTINCT user_id,
                                    toDate(time) AS day
                                FROM simulator_20230320.feed_actions
                                ) t2 
                              USING user_id
                               WHERE start_day> toDate(yesterday()) + INTERVAL -7 DAY  AND
                                      start_day<= toDate(yesterday())
                              )
                            GROUP BY start_day, day
                    format TSVWithNames
                 """
        df_coh_feed = execute_query(query_cohort_feed)
        df_coh_feed['start_day'] = df_coh_feed['start_day'].apply(lambda x : pd.to_datetime(str(x))).dt.date
        df_coh_feed['day'] = df_coh_feed['day'].apply(lambda x : pd.to_datetime(str(x))).dt.date
        return df_coh_feed
    
    @task()
    # Готовим данные из summary для feeds
    def transform_summary_feed(df_yesterday,df_week_ago, df_month_ago):
        yesterday_users = df_yesterday[['users_feed','users_messanger','hour','avg_likes_b_user', 'avg_views_b_user','CTR']]
        week_ago_users = df_week_ago[['users_feed','users_messanger','hour','avg_likes_b_user', 'avg_views_b_user','CTR']]
        month_ago_users = df_month_ago[['users_feed','users_messanger','hour','avg_likes_b_user','avg_views_b_user','CTR']]

        df_graphic_dau = pd.merge(pd.merge(yesterday_users, week_ago_users, on = 'hour', suffixes=('_y', '_w')), month_ago_users, on = 'hour')
        return df_graphic_dau
    @task()
    # Готовим данные из summary для messanger
    def transform_summary_messanger(df_yesterday,df_week_ago, df_month_ago):
        yesterday_users = df_yesterday[['users_messanger','hour','avg_msg_send','avg_recievers']]
        week_ago_users = df_week_ago[['users_messanger','hour','avg_msg_send','avg_recievers']]
        month_ago_users = df_month_ago[['users_messanger','hour','avg_msg_send','avg_recievers']]

        df_graphic_messanger = pd.merge(pd.merge(yesterday_users, week_ago_users, on = 'hour', suffixes=('_y', '_w')), month_ago_users, on = 'hour')
        return df_graphic_messanger
    
    @task()
    # Выводы по KPI лента новостей
    def conclusions_kpi_feed(df_graphic_dau):
        def check_plus_minus(value):
            if value > 0:
                return "больше"
            else:
                return "меньше"
        # DAU
        avg_users_feed_y = df_graphic_dau.users_feed_y.mean().round(2)
        avg_users_feed_w = df_graphic_dau.users_feed_w.mean()
        avg_users_feed_m = df_graphic_dau.users_feed.mean()
        # % отклонения от yesterday
        y_vs_w_users_feed = (((avg_users_feed_y - avg_users_feed_w)/avg_users_feed_w)*100).round(2)
        y_vs_m_users_feed = (((avg_users_feed_y - avg_users_feed_m)/avg_users_feed_m)*100).round(2)
        # Заключение к DAU feed
        conclusion_dau_feed = f" Вывод по KPI за вчерашний день: Среднее DAU ленты новостей  = {avg_users_feed_y} пользователей, что на {y_vs_w_users_feed} % {check_plus_minus(y_vs_m_users_feed)} , чем показатель неделю назад. И на {y_vs_m_users_feed} % {check_plus_minus(y_vs_m_users_feed)}, чем месяц назад."


        # likes per user
        likes_per_user_feed_y = df_graphic_dau.avg_likes_b_user_y.mean().round(2)
        likes_per_user_feed_w = df_graphic_dau.avg_likes_b_user_w.mean()
        likes_per_user_feed_m = df_graphic_dau.avg_likes_b_user.mean()
        # % отклонения от yesterday
        y_vs_w_likes_per_user_feed = (((likes_per_user_feed_y - likes_per_user_feed_w)/likes_per_user_feed_w)*100).round(2)
        y_vs_m_likes_per_user_feed = (((likes_per_user_feed_y - likes_per_user_feed_m)/likes_per_user_feed_m)*100).round(2)
        # Заключение к DAU feed
        conclusion_likes_per_user_feed = f" Среднее количество лайков на пользователя  = {likes_per_user_feed_y}, что на {y_vs_w_likes_per_user_feed} % {check_plus_minus(y_vs_w_likes_per_user_feed)} , чем показатель неделю назад. И на {y_vs_m_likes_per_user_feed} % {check_plus_minus(y_vs_m_likes_per_user_feed)}, чем показатель месячной давности."


        # views per user
        views_per_user_feed_y = df_graphic_dau.avg_views_b_user_y.mean().round(2)
        views_per_user_feed_w = df_graphic_dau.avg_views_b_user_w.mean()
        views_per_user_feed_m = df_graphic_dau.avg_views_b_user.mean()
        # % отклонения от yesterday
        y_vs_w_views_per_user_feed = (((views_per_user_feed_y - views_per_user_feed_w)/views_per_user_feed_w)*100).round(2)
        y_vs_m_views_per_user_feed = (((views_per_user_feed_y - views_per_user_feed_m)/views_per_user_feed_m)*100).round(2)
        # Заключение к views per user
        conclusion_views_per_user_feed = f" Среднее количество просмотров на пользователя  = {views_per_user_feed_y}, что на {y_vs_w_views_per_user_feed} % {check_plus_minus(y_vs_w_views_per_user_feed)} , чем показатель неделю назад. И на {y_vs_m_views_per_user_feed} % {check_plus_minus(y_vs_m_views_per_user_feed)}, чем показатель месяц назад."


        # avg CTR
        ctr_feed_y = df_graphic_dau.CTR_y.mean().round(2)
        ctr_feed_w = df_graphic_dau.CTR_w.mean()
        ctr_feed_m = df_graphic_dau.CTR.mean()
        # % отклонения от yesterday
        y_vs_w_ctr = (((ctr_feed_y - ctr_feed_w )/ctr_feed_w )*100).round(2)
        y_vs_m_ctr = (((ctr_feed_y - ctr_feed_m)/ctr_feed_m)*100).round(2)
        # Заключение к CTR
        conclusion_ctr = f" CTR  = {ctr_feed_y}, что на {y_vs_w_ctr} % {check_plus_minus(y_vs_w_ctr)} , чем показатель неделю назад. И на {y_vs_m_ctr} % {check_plus_minus(y_vs_m_ctr)}, чем показатель месячной давности."

        return f"{conclusion_dau_feed} {conclusion_likes_per_user_feed} {conclusion_views_per_user_feed} {conclusion_ctr}"

    @task()
    # Графики мониторинг ленты новостей
    def graphic_feed_kpi(df_graphic_dau):
        data = df_graphic_dau
        fig, axes = plt.subplots(1,4, figsize=(20, 10))
        plt.xticks(rotation=45)
        fig.suptitle('Мониторинг KPI ленты новостей по часам в сравнении (yesterday vs. week ago vs. month ago)')

        # DAU
        ax= sns.lineplot(ax=axes[0], x='hour', y='users_feed_y', data=data, sort = True , label = 'yesterday').set(title='DAU')
        ax1 = sns.lineplot(ax=axes[0], x='hour', y='users_feed_w', data=data, sort = True, label = 'week ago', linestyle='dashed')
        ax2 = sns.lineplot(ax=axes[0], x='hour', y='users_feed', data=data, sort = True, label = 'month ago', linestyle= 'dotted')
        # likes per user
        ax= sns.lineplot(ax=axes[1], x='hour', y='avg_likes_b_user_y', data=data, sort = True , label = 'yesterday').set(title='likes per user')
        ax1 = sns.lineplot(ax=axes[1], x='hour', y='avg_likes_b_user_w', data=data, sort = True, label = 'week ago', linestyle='dashed')
        ax2 = sns.lineplot(ax=axes[1], x='hour', y='avg_likes_b_user', data=data, sort = True, label = 'month ago', linestyle= 'dotted')
        # views per user
        ax= sns.lineplot(ax=axes[2], x='hour', y='avg_views_b_user_y', data=data, sort = True , label = 'yesterday').set(title='views per user')
        ax1 = sns.lineplot(ax=axes[2], x='hour', y='avg_views_b_user_w', data=data, sort = True, label = 'week ago', linestyle='dashed')
        ax2 = sns.lineplot(ax=axes[2], x='hour', y='avg_views_b_user', data=data, sort = True, label = 'month ago', linestyle= 'dotted')
        # avg CTR
        ax= sns.lineplot(ax=axes[3], x='hour', y='CTR_y', data=data, sort = True , label = 'yesterday').set(title='avg CTR')
        ax1 = sns.lineplot(ax=axes[3], x='hour', y='CTR_w', data=data, sort = True, label = 'week ago', linestyle='dashed')
        ax2 = sns.lineplot(ax=axes[3], x='hour', y='CTR', data=data, sort = True, label = 'month ago', linestyle= 'dotted')

        plt.title('kpi_feed')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'kpi_feed.png'
        plt.close()
        return plot_object

    @task()
    # Выводы по KPI мессанджера
    def conclusion_kpi_messanger(df_graphic_messanger):
        def check_plus_minus(value):
            if value > 0:
                return "больше"
            else:
                return "меньше"
        # DAU
        avg_users_messanger_y = df_graphic_messanger.users_messanger_y.mean().round(2)
        avg_users_messanger_w = df_graphic_messanger.users_messanger_w.mean()
        avg_users_messanger_m = df_graphic_messanger.users_messanger.mean()
        # % отклонения от yesterday
        y_vs_w_users_messanger = (((avg_users_messanger_y - avg_users_messanger_w)/avg_users_messanger_w)*100).round(2)
        y_vs_m_users_messanger = (((avg_users_messanger_y - avg_users_messanger_m)/avg_users_messanger_m)*100).round(2)
        # Заключение к DAU messanger
        conclusion_dau_messanger = f" Вывод по KPI за вчерашний день: Среднее DAU мессанджера  = {avg_users_messanger_y} пользователей,\
        что на {y_vs_w_users_messanger} % {check_plus_minus(y_vs_w_users_messanger)} , чем показатель неделю назад. И на {y_vs_m_users_messanger} %\
        {check_plus_minus(y_vs_m_users_messanger)}, чем месяц назад."
        
        # messages sent per user
        avg_msg_send_y = df_graphic_messanger.avg_msg_send_y.mean().round(2)
        avg_msg_send_w = df_graphic_messanger.avg_msg_send_w.mean()
        avg_msg_send_m = df_graphic_messanger.avg_msg_send.mean()
        # % отклонения от yesterday
        y_vs_w_avg_msg_send = (((avg_msg_send_y - avg_msg_send_w)/avg_msg_send_w)*100).round(2)
        y_vs_m_avg_msg_send = (((avg_msg_send_y - avg_msg_send_m)/avg_msg_send_m)*100).round(2)
        # Заключение к messages sent per user
        conclusion_msg_send = f" Среднее количество отправлеяемых сообщений пользователем  = {avg_msg_send_y}, что на\
        {y_vs_w_avg_msg_send} % {check_plus_minus(y_vs_w_avg_msg_send)} , чем показатель неделю назад. И на {y_vs_m_avg_msg_send} % \
        {check_plus_minus(y_vs_m_avg_msg_send)}, чем показатель месячной давности."
        
        # users whom sent msg Скольким пользователям отправили сообщение - **users_sent**
        avg_users_sent_y = df_graphic_messanger.avg_recievers_y.mean().round(2)
        avg_users_sent_w = df_graphic_messanger.avg_recievers_w.mean()
        avg_users_sent_m = df_graphic_messanger.avg_recievers.mean()
        # % отклонения от yesterday
        y_vs_w_users_sent = (((avg_users_sent_y - avg_users_sent_w)/avg_users_sent_w)*100).round(2)
        y_vs_m_users_sent = (((avg_users_sent_y - avg_users_sent_m)/avg_users_sent_m)*100).round(2)
        # Заключение к views per user
        conclusion_users_sent = f" Среднее количество собеседников у пользователя  = {avg_users_sent_y}, что на {y_vs_w_users_sent} %\
        {check_plus_minus(y_vs_w_users_sent)} , чем показатель неделю назад. И на {y_vs_m_users_sent} % {check_plus_minus(y_vs_m_users_sent)}, чем показатель месяц назад."
        
        return f"{conclusion_dau_messanger} {conclusion_msg_send} {conclusion_users_sent}"
        
    @task()
    # Графики мониторинг мессанджера
    def graphic_messanger_kpi(df_graphic_messanger):
        data = df_graphic_messanger
        fig, axes = plt.subplots(1,3, figsize=(20, 10))
        plt.xticks(rotation=45)
        fig.suptitle('Мониторинг KPI мессанджера по часам в сравнении (yesterday vs. week ago vs. month ago)')

        # DAU
        ax= sns.lineplot(ax=axes[0], x='hour', y='users_messanger_y', data=data, sort = True , label = 'yesterday').set(title='DAU')
        ax1 = sns.lineplot(ax=axes[0], x='hour', y='users_messanger_w', data=data, sort = True, label = 'week ago', linestyle='dashed')
        ax2 = sns.lineplot(ax=axes[0], x='hour', y='users_messanger', data=data, sort = True, label = 'month ago', linestyle= 'dotted')

        # avg messages sent per user
        ax= sns.lineplot(ax=axes[1], x='hour', y='avg_msg_send_y', data=data, sort = True , label = 'yesterday').set(title='messages sent per user')
        ax1 = sns.lineplot(ax=axes[1], x='hour', y='avg_msg_send_w', data=data, sort = True, label = 'week ago', linestyle='dashed')
        ax2 = sns.lineplot(ax=axes[1], x='hour', y='avg_msg_send', data=data, sort = True, label = 'month ago', linestyle= 'dotted')

        # unique users whom sent msg
        ax= sns.lineplot(ax=axes[2], x='hour', y='avg_recievers_y', data=data, sort = True , label = 'yesterday').set(title='users whom sent msg')
        ax1 = sns.lineplot(ax=axes[2], x='hour', y='avg_recievers_w', data=data, sort = True, label = 'week ago', linestyle='dashed')
        ax2 = sns.lineplot(ax=axes[2], x='hour', y='avg_recievers', data=data, sort = True, label = 'month ago', linestyle= 'dotted')
        
        plt.title('kpi_messanger')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'kpi_messanger.png'
        plt.close()
        return plot_object

    @task()
    # Hearmap лента новостей
    def heatmap_feed(df_coh_feed):
        cohort_feed = df_coh_feed.pivot_table(index = 'start_day',
                                     columns = 'period',
                                     values = 'users')
        cohort_feed_size = cohort_feed.iloc[:,0]
        retention_feed = cohort_feed.divide(cohort_feed_size , axis = 0).round(3)
        # HEatmap в %
        graphic_heatmap_feed = (retention_feed
                    .style
                    .set_caption('Когортный анализ пользователей новостной ленты за последнюю неделю (%)')  # добавляем подпись
                    .background_gradient(cmap='Reds')  # раскрашиваем ячейки по столбцам
                    .highlight_null('white')  # делаем белый фон для значений NaN
                    .format("{:.1%}", na_rep=""))  # числа форматируем как проценты, NaN заменяем на пустоту
        graphic_heatmap_feed

        plt.title('heatmap_feed')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'heatmap_feed.png'
        plt.close()
        return plot_object

    @task()
    # Heatmap мессанджер
    def heatmap_messanger(df_coh_messager):
        cohort_messanger = df_coh_messager.pivot_table(index = 'start_date',
                                     columns = 'period',
                                     values = 'users')
        cohort_messanger_size = cohort_messanger.iloc[:,0]
        retention_messanger = cohort_messanger.divide(cohort_messanger_size , axis = 0).round(3)
        graphic_heatmap_messanger = (retention_messanger
                    .style
                    .set_caption('Когортный анализ пользователей мессанджера за последнюю неделю (%)')  # добавляем подпись
                    .background_gradient(cmap='Reds')  # раскрашиваем ячейки по столбцам
                    .highlight_null('white')  # делаем белый фон для значений NaN
                    .format("{:.1%}", na_rep=""))  # числа форматируем как проценты, NaN заменяем на пустоту
        graphic_heatmap_messanger
        plt.title('heatmap_messanger')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'heatmap_messanger.png'
        plt.close()
        return plot_object

    @task
    def send_results(
        graphic_feed_kpi_, conclusions_kpi_feed_, graphic_messanger_kpi_, conclusion_kpi_messanger_, heatmap_feed_, heatmap_messanger_
    ):
        bot.sendPhoto(chat_id=chat_id, photo=graphic_feed_kpi_)
        sleep(1)
        bot.sendMessage(chat_id=chat_id, text = conclusions_kpi_feed_)
        sleep(1)
        bot.sendPhoto(chat_id=chat_id, photo=graphic_messanger_kpi_)
        sleep(1)
        bot.sendMessage(chat_id=chat_id, text = conclusion_kpi_messanger_)
        sleep(1)
        bot.sendPhoto(chat_id=chat_id, photo=heatmap_feed_)
        sleep(1)
        bot.sendPhoto(chat_id=chat_id, photo=heatmap_messanger_)


# Перенести в конец
    # EXTRACT
    df_yesterday = extract_summary_yesterday()
    df_week_ago = extract_summary_week_ago()
    df_month_ago = extract_summary_month_ago()
    df_coh_messager = extract_cohort_messager()
    df_coh_feed = extract_cohort_feed()

    # TRANSFORM
    df_graphic_dau = transform_summary_feed(df_yesterday,df_week_ago, df_month_ago)
    df_graphic_messanger = transform_summary_messanger(df_yesterday,df_week_ago, df_month_ago)
    
    # LOAD
    graphic_feed_kpi_ = graphic_feed_kpi(df_graphic_dau)
    conclusions_kpi_feed_ = conclusions_kpi_feed(df_graphic_dau)
    graphic_messanger_kpi_ = graphic_messanger_kpi(df_graphic_messanger)
    conclusion_kpi_messanger_ = conclusion_kpi_messanger(df_graphic_messanger)
    
    heatmap_feed_ = heatmap_feed(df_coh_feed)
    heatmap_messanger_ = heatmap_messanger(df_coh_messager)

    send_results(graphic_feed_kpi_, conclusions_kpi_feed_, graphic_messanger_kpi_, conclusion_kpi_messanger_, heatmap_feed_, heatmap_messanger_)
    

# Перенести в конец
dag_agrova_airflow_3 = dag_agrova_airflow_3()
