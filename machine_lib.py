import asyncio
import logging
import requests
from os import environ
from time import sleep
import time
import json
import pandas as pd
import random
import pickle
from itertools import product
from itertools import combinations
from collections import defaultdict
from urllib.parse import urljoin
from datetime import datetime
import pickle
import asyncio
import wqb
from wqb import FilterRange
from wqb import WQBSession, print

import concurrent
# import concurrent.futures
import numpy as np
import requests
from os import environ
from time import sleep
import time
import json
import pandas as pd
import datetime
import random
import pickle
from itertools import product
from itertools import combinations
from collections import defaultdict
from urllib.parse import urljoin
import pickle
import glob
import os
import re
import threading
import time
from os.path import expanduser
# 将添加获得权限的Vector操作符添加在此处
import logging

# from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, as_completed
import threading
from queue import Queue
from time import sleep
from tqdm import tqdm

# 将添加获得权限的Vector操作符添加在此处
 
basic_ops = ["reverse", "inverse", "rank", "zscore", "quantile", "normalize"]
 
ts_ops = ["ts_rank", "ts_zscore", "ts_delta",  "ts_sum", "ts_delay", 
          "ts_std_dev", "ts_mean",  "ts_arg_min", "ts_arg_max","ts_scale", "ts_quantile"]
 
ops_set = basic_ops + ts_ops 

# config.py

# 定义全局变量 groups
groups = [
    "market", "sector", "industry", "subindustry", "exchange",
    "bucket(rank(cap), range='0.1, 1, 0.1')",
    "bucket(rank(cap), range='0, 1, 0.5')",
    "bucket(rank(assets), range='0.1, 1, 0.1')",
    "bucket(group_rank(cap, sector), range='0.1, 1, 0.1')",
    "bucket(group_rank(assets, sector), range='0.1, 1, 0.1')",
    "bucket(rank(ts_std_dev(returns, 20)), range='0.1, 1, 0.1')",
    "bucket(rank(close * volume), range='0.1, 1, 0.1')",
    "bucket(rank(split), range='0.1, 1, 0.1')"
]

def login():
    
# Load credentials # 加载凭证
    with open(expanduser('brain_credentials.txt')) as f:
        credentials = json.load(f)

    # Extract username and password from the list # 从列表中提取用户名和密码
    username, password = credentials
 
    # Create a session to persistently store the headers
    s = requests.Session()
 
    # Save credentials into session
    s.auth = (username, password)
 
    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    print(response.content)
    return s  


def get_datasets(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000'
):
    url = "https://api.worldquantbrain.com/data-sets?" +\
        f"instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
    result = s.get(url)
    datasets_df = pd.DataFrame(result.json()['results'])
    return datasets_df


def get_datafields(
    s,  #：一个 requests.Session 对象，用于保持会话状态。
    instrument_type: str = 'EQUITY',
    region: str = '',
    delay: int = 1,
    universe: str = '',
    dataset_id: str = '',
    search: str = ''
):
    if len(search) == 0:
        # 如果没有指定搜索关键字，则使用默认的 URL 模板，并从 API 获取总记录数（count）。
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&order=alphaCount&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" +\
            "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count'] 
        
    else:
        # 如果指定了搜索关键字，则直接设置 count = 100，表示每次请求最多返回 100 条记录。
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" +\
            f"&search={search}" +\
            "&offset={x}"
        count = 2500
    
    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])
 
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
 
    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df


def get_vec_fields(fields):

    # 请在此处添加获得权限的Vector操作符
    vec_ops = ["vec_avg", "vec_sum"]
    vec_fields = []
 
    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)"%(vec_op, field))
                vec_fields.append("%s(%s, nth=0)"%(vec_op, field))
            else:
                vec_fields.append("%s(%s)"%(vec_op, field))
 
    return(vec_fields)


def process_datafields(df):

    datafields = []
    datafields += df[df['type'] == "MATRIX"]["id"].tolist()
    datafields += get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
    # return ["winsorize(ts_backfill(%s, 120), std=4)"%field for field in datafields]
    return datafields


def ts_factory(op, field):
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 64, 128, 256]
    
    for day in days:
    
        alpha = "%s(%s, %d)"%(op, field, day)
        output.append(alpha)
    
    return output

def first_order_factory(fields, ops_set):
    alpha_set = []
    # for field in fields:
    for field in fields:
        #reverse op does the work
        alpha_set.append(field)
        #alpha_set.append("-%s"%field)
        for op in ops_set:
 
            if op == "ts_percentage":
 
                alpha_set += ts_comp_factory(op, field, "percentage", [0.5])
 
 
            elif op == "ts_decay_exp_window":
 
                alpha_set += ts_comp_factory(op, field, "factor", [0.5])
 
 
            elif op == "ts_moment":
 
                alpha_set += ts_comp_factory(op, field, "k", [2, 3, 4])
 
            elif op == "ts_entropy":
 
                alpha_set += ts_comp_factory(op, field, "buckets", [10])
 
            elif op.startswith("ts_") or op == "inst_tvr":
 
                alpha_set += ts_factory(op, field)
 
            elif op.startswith("vector"):
 
                alpha_set += vector_factory(op, field)
 
            elif op == "signed_power":
 
                alpha = "%s(%s, 2)"%(op, field)
                alpha_set.append(alpha)
 
            else:
                alpha = "%s(%s)"%(op, field)
                alpha_set.append(alpha)
    # for field1 in fields:
    #     # for field2 in fields:
    #             alpha_set.append(f"-ts_regression(ts_delta(cap,1),{field1},200)")

    #             alpha_set.append(f"trade_when(ts_corr(close, volume, 20) > 0.5, group_backfill(winsorize(ts_backfill({field1},5),std=4),market,22, std=4.0), -1)")
                
   
    
                
    return alpha_set

def load_task_pool_single(alpha_list, limit_of_single_simulations):

    '''
    Input:
        alpha_list : list of (alpha, decay) tuples
        limit_of_single_simulations : number of concurrent single simulations
    Output:
        task : [3 * (alpha, decay)] for 3 single simulations
        pool : [ alpha_num/3 * [3 * (alpha, decay)] ] 
    '''

    pool = [alpha_list[i:i + limit_of_single_simulations] for i in range(0, len(alpha_list), limit_of_single_simulations)]
    # pool = alpha_list
    return pool


def single_simulate(alpha_pool, neut, region, universe, start):

    s = login()
    print(f"Task start in at: {datetime.datetime.now()}")

    brain_api_url = 'https://api.worldquantbrain.com'

    for x, task in enumerate(alpha_pool):
        if x < start: continue
        progress_urls = []
        for y, (alpha, decay) in enumerate(task):

            # 3 single alphas in each task
            simulation_data = {
                'type': 'REGULAR',
                'settings': {
                    'instrumentType': 'EQUITY',
                    'region': region, 
                    'universe': universe, 
                    'delay': 1,
                    'decay': decay, 
                    'neutralization': neut,
                    'truncation': 0.08,
                    'pasteurization': 'ON',
                    'testPeriod': 'P0Y',
                    'unitHandling': 'VERIFY',
                    'nanHandling': 'ON',
                    'language': 'FASTEXPR',
                    'visualization': False,
                },
            'regular': alpha}

            try:
                simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=simulation_data)
                simulation_progress_url = simulation_response.headers['Location']
                progress_urls.append(simulation_progress_url)
            except:
                print("location key error: %s"%simulation_response.content)
                sleep(600)
                s = login()

        print("task %d post done"%(x))

        for j, progress in enumerate(progress_urls):
            try:
                while True:
                    simulation_progress = s.get(progress)
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    #print("Sleeping for " + simulation_progress.headers["Retry-After"] + " seconds")
                    # 如果响应头中包含 "Retry-After" 字段，则表示需要等待一段时间后重试。
                    # 程序会根据 "Retry-After" 的值调用 sleep 函数暂停执行，避免频繁请求。
                    sleep(float(simulation_progress.headers["Retry-After"]))

                status = simulation_progress.json().get("status", 0)  #判断任务的状态，如果为 "COMPLETE" 或 "WARNING"，则跳出循环。
                if status != "COMPLETE" and status != "WARNING":
                    print("Not complete : %s"%(progress))

                """
                alpha_id = simulation_progress.json()["alpha"]

                set_alpha_properties(s,
                        alpha_id,
                        name = "%s"%name,
                        color = None,)
                """
            except KeyError:
                print("look into: %s"%progress)
            except:
                print("other")

        print("task %d simulate done"%(x))
        print(f"Logged in at: {datetime.datetime.now()}")
    
    print(f"Simulate done in at: {datetime.datetime.now()}")

def set_alpha_properties(
    s,
    alpha_id,
    name: str = None,
    color: str = None,
    selection_desc: str = "None",
    combo_desc: str = "None",
    tags: str = ["ace_tag"],
):
    """
    Function changes alpha's description parameters
    """
 
    params = {
        "color": color,
        "name": name,
        "tags": tags,
        "category": None,
        "regular": {"description": None},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }
    response = s.patch(
        "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
    )

# 北京时间转换为美东时间
def get_est_time(original_str):   
    """
    # 北京时间转换为美国东部时间
    original_str:  2025-01-11 12:00:00  
                  or   2025-01-11
    """
    from datetime import datetime
    import zoneinfo
    if len(original_str) == 10:  #if argument is only date not time, will add the default time.
        original_str = original_str + ' 00:00:00'
    try:
        original_dt = datetime.strptime(original_str, "%Y-%m-%d %H:%M:%S")
    except:
        print(f"error when convert date from string to date {original_str}")
        return
    # except:
    #     # 输入：2025-01-11
    #     original_dt = datetime.strptime(original_str, "%Y-%m-%d")
    original_dt = original_dt.replace(tzinfo=zoneinfo.ZoneInfo("Asia/Shanghai"))
    converted_dt = original_dt.astimezone(zoneinfo.ZoneInfo("America/New_York"))
    return converted_dt.isoformat()

def get_alphas(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage):

    
    s = login()

    start_date = get_est_time(start_date)
    end_date = get_est_time(end_date)

    output = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 100):
        print(i)
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
            + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=" + start_date  \
            + "&dateCreated%3C" + end_date \
            + "&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
            + str(sharpe_th) + "&settings.region=" + region + "&order=-is.sharpe&hidden=false&type!=SUPER"
        
        url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
            + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=" + start_date  \
            + "&dateCreated%3C" + end_date \
            + "&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
            + str(sharpe_th) + "&settings.region=" + region + "&order=is.sharpe&hidden=false&type!=SUPER"
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            response = s.get(url)
            #print(response.json())
            try:
                alpha_list = response.json()["results"]
                #print(response.json())
                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    truncation = alpha_list[j]["settings"]["truncation"]
                    exp = alpha_list[j]['regular']['code']
                    count += 1
                    #if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
                    if (longCount + shortCount) > 100:
                        if sharpe < -sharpe_th:
                            exp = "-%s"%exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay,truncation]
                        print(rec)
                        if turnover > 0.7:
                            rec.append(decay*4)
                        elif turnover > 0.6:
                            rec.append(decay*3+3)
                        elif turnover > 0.5:
                            rec.append(decay*3)
                        elif turnover > 0.4:
                            rec.append(decay*2)
                        elif turnover > 0.35:
                            rec.append(decay+4)
                        elif turnover > 0.3:
                            rec.append(decay+2)
                        output.append(rec)
            except:
                print("%d finished re-login"%i)
                s = login()

    print("count: %d"%count)
    return output

def prune(next_alpha_recs, prefix, keep_num):
    # prefix is the datafield prefix, fnd6, mdl175 ...
    # keep_num is the num of top sharpe same-datafield alpha
    # output = []
    # num_dict = defaultdict(int)
    # for rec in next_alpha_recs:
    #     exp = rec[1]
    #     field = exp.split(prefix)[-1].split(",")[0]
    #     sharpe = rec[2]
    #     if sharpe < 0:
    #         field = "-%s"%field
    #     if num_dict[field] < keep_num:
    #         num_dict[field] += 1
    #         decay = rec[-1]
    #         exp = rec[1]
    #         output.append([exp,decay])
    # output =  [[rec[1], rec[7]] for rec in next_alpha_recs]
    output = []  # 用于存储筛选后的Alpha表达式和衰减参数
    # num_dict = defaultdict(int)  # 用于记录每个数据字段已保留的Alpha表达式数量

    for rec in next_alpha_recs:
        try:
            exp = rec[1]  # 获取Alpha表达式
            if not exp.startswith(prefix):
                 output.append([rec[1], rec[7]]) 
         
        except Exception as e:
            logging.error(f"Error processing record {rec}: {e}")

    return output

def get_group_second_order_factory(first_order, group_ops, region):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, region)
    return second_order


def group_factory(op, field, region):
    output = []
    vectors = ["cap"] 
    
    usa_group_13 = ['pv13_h_min2_3000_sector','pv13_r2_min20_3000_sector','pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector','pv13_hierarchy_min2_focused_pureplay_3000_513_sector',
                    'oth455_competitor_n2v_p10_q50_w1_kmeans_cluster_10','oth455_customer_n2v_p10_q50_w5_kmeans_cluster_10',
                    'oth455_relation_n2v_p50_q200_w5_kmeans_cluster_20','oth455_relation_n2v_p50_q50_w3_pca_fact2_cluster_10']
    
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    asset_group = "bucket(rank(assets),range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap, sector),range='0.1, 1, 0.1')"
    sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"

    vol_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"

    liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"
    split_group = "bucket(rank(split),range = '0.1, 1, 0.1')"

    groups = ["market","sector", "industry", "subindustry","exchange",
              cap_group, asset_group, sector_cap_group, sector_asset_group, vol_group, liquidity_group,split_group]
    
    groups += usa_group_13
        
    for group in groups:
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha = "%s(%s,%s,densify(%s))"%(op, field, vector, group)
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha = "%s(%s,densify(%s),percentage=0.5)"%(op, field, group)
            output.append(alpha)
        else:
            alpha = "%s(%s,densify(%s))"%(op, field, group)
            output.append(alpha)
        
    return output

def trade_when_factory(op,field,region):
    output = []
    open_events = ["ts_arg_max(volume, 5) == 0", "ts_corr(close, volume, 20) < 0",
                   "ts_corr(close, volume, 5) < 0", "ts_mean(volume,10)>ts_mean(volume,60)",
                   "group_rank(ts_std_dev(returns,60), sector) > 0.7", "ts_zscore(returns,60) > 2",
                   "ts_arg_min(volume, 5) > 3",
                   "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
                   "ts_arg_max(close, 5) == 0", "ts_arg_max(close, 20) == 0",
                   "ts_corr(close, volume, 5) > 0", "ts_corr(close, volume, 5) > 0.3", "ts_corr(close, volume, 5) > 0.5",
                   "ts_corr(close, volume, 20) > 0", "ts_corr(close, volume, 20) > 0.3", "ts_corr(close, volume, 20) > 0.5",
                   "ts_regression(returns, %s, 5, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, %s, 20, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
                   "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0",
                   "volume < multiply(adv20, 0.8),"
                   "volume > multiply(adv20, 3.0)"]

    exit_events = ["abs(returns) > 0.1", "-1","ts_sum(if_else(returns<-0.83,1,0),5)>2",
                   "ts_sum(if_else(returns<8,1,0),5)> 3"]

    usa_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8", "rank(vec_avg(mws82_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mws82_sentiment),22) > 0.8", "rank(vec_avg(nws48_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws48_ssc),22) > 0.8", "rank(vec_avg(mws50_ssc)) > 0.8", "ts_rank(vec_avg(mws50_ssc),22) > 0.8",
                  "ts_rank(vec_sum(scl12_alltype_buzzvec),22) > 0.9", "pcr_oi_270 < 1", "pcr_oi_270 > 1",]

    asi_events = ["rank(vec_avg(mws38_score)) > 0.8", "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    eur_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
                  "rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos)) > 0.8",
                  "ts_rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos),22) > 0.8",
                  "rank(vec_avg(mws84_sentiment)) > 0.8", "ts_rank(vec_avg(mws84_sentiment),22) > 0.8",
                  "rank(vec_avg(mws85_sentiment)) > 0.8", "ts_rank(vec_avg(mws85_sentiment),22) > 0.8",
                  "rank(mdl110_analyst_sentiment) > 0.8", "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
                  "rank(vec_avg(nws3_scores_posnormscr)) > 0.8",
                  "ts_rank(vec_avg(nws3_scores_posnormscr),22) > 0.8",
                  "rank(vec_avg(mws36_sentiment_words_positive)) > 0.8",
                  "ts_rank(vec_avg(mws36_sentiment_words_positive),22) > 0.8"]

    glb_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(vec_avg(nws20_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws20_ssc),22) > 0.8",
                  "vec_avg(nws20_ssc) > 0",
                  "rank(vec_avg(nws20_bee)) > 0.8",
                  "ts_rank(vec_avg(nws20_bee),22) > 0.8",
                  "rank(vec_avg(nws20_qmb)) > 0.8",
                  "ts_rank(vec_avg(nws20_qmb),22) > 0.8"]

    chn_events = ["rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform),22) > 0.8"]

    kor_events = ["rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mdl110_analyst_sentiment),22) > 0.8",
                  "rank(vec_avg(mws38_score)) > 0.8",
                  "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    twn_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(rp_ess_business) > 0.8",
                  "ts_rank(rp_ess_business,22) > 0.8"]

    for oe in open_events:
        for ee in exit_events:
            alpha = "%s(%s, %s, %s)"%(op, oe, field, ee)
            output.append(alpha)
    return output


def check_submission(alpha_bag, gold_bag, start):
    depot = []
    s = login()
    for idx, g in enumerate(alpha_bag):
        if idx < start:
            continue
        if idx % 5 == 0:
            print(idx)
        if idx % 200 == 0:
            s = login()
        #print(idx)
        pc = get_check_submission(s, g)
        if pc == "sleep":
            sleep(100)
            s = login()
            alpha_bag.append(g)
        elif pc != pc:
            # pc is nan
            print("check self-corrlation error")
            sleep(100)
            alpha_bag.append(g)
        elif pc == "fail":
            continue
        elif pc == "error":
            depot.append(g)
        else:
            print(g)
            gold_bag.append((g, pc))
    print(depot)
    return gold_bag


def get_check_submission(s, alpha_id):
    while True:
        result = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/check")
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    try:
        if result.json().get("is", 0) == 0:
            print("logged out")
            return "sleep"
        checks_df = pd.DataFrame(
                result.json()["is"]["checks"]
        )
        pc = checks_df[checks_df.name == "SELF_CORRELATION"]["value"].values[0]
        if not any(checks_df["result"] == "FAIL"):
            return pc
        else:
            return "fail"
    except:
        print("catch: %s"%(alpha_id))
        return "error"
    
def view_alphas(gold_bag):
    s = login()
    sharp_list = []
    for gold, pc in gold_bag:

        triple = locate_alpha(s, gold)
        info = [triple[0], triple[2], triple[3], triple[4], triple[5], triple[6], triple[1]]
        info.append(pc)
        sharp_list.append(info)

    sharp_list.sort(reverse=True, key = lambda x : x[1])
    for i in sharp_list:
        print(i)
 
def locate_alpha(s, alpha_id):
    while True:
        alpha = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
        if "retry-after" in alpha.headers:
            time.sleep(float(alpha.headers["Retry-After"]))
        else:
            break
    string = alpha.content.decode('utf-8')
    metrics = json.loads(string)
    #print(metrics["regular"]["code"])
    
    dateCreated = metrics["dateCreated"]
    sharpe = metrics["is"]["sharpe"]
    fitness = metrics["is"]["fitness"]
    turnover = metrics["is"]["turnover"]
    margin = metrics["is"]["margin"]
    decay = metrics["settings"]["decay"]
    exp = metrics['regular']['code']
    
    triple = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
    return triple
            

# Consultant methods
def multi_simulate(alpha_pools, neut, region, universe, start):

    s = login()

    brain_api_url = 'https://api.worldquantbrain.com'

    for x, pool in enumerate(alpha_pools):
        if x < start: continue
        progress_urls = []
        for y, task in enumerate(pool):
            # 10 tasks, 10 alpha in each task
            sim_data_list = generate_sim_data(task, region, universe, neut)
            try:
                simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=sim_data_list)
                simulation_progress_url = simulation_response.headers['Location']
                progress_urls.append(simulation_progress_url)
            except:
                print("loc key error: %s"%simulation_response.content)
                sleep(600)
                s = login()

        print("pool %d task %d post done"%(x,y))

        for j, progress in enumerate(progress_urls):
            try:
                while True:
                    simulation_progress = s.get(progress)
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    #print("Sleeping for " + simulation_progress.headers["Retry-After"] + " seconds")
                    sleep(float(simulation_progress.headers["Retry-After"]))

                status = simulation_progress.json().get("status", 0)
                if status != "COMPLETE":
                    print("Not complete : %s"%(progress))

                """
                #alpha_id = simulation_progress.json()["alpha"]
                children = simulation_progress.json().get("children", 0)
                children_list = []
                for child in children:
                    child_progress = s.get(brain_api_url + "/simulations/" + child)
                    alpha_id = child_progress.json()["alpha"]

                    set_alpha_properties(s,
                            alpha_id,
                            name = "%s"%name,
                            color = None,)
                """
            except KeyError:
                print("look into: %s"%progress)
            except:
                print("other")


        print("pool %d task %d simulate done"%(x, j))
    
    print("Simulate done")

def generate_sim_data(alpha_list, region, uni, neut):
    sim_data_list = []
    for alpha, decay in alpha_list:
        simulation_data = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': uni,
                'delay': 1,
                'decay': decay,
                'neutralization': neut,
                'truncation': 0.08,
                'pasteurization': 'ON',
                'testPeriod': 'P2Y',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
            },
            'regular': alpha}

        sim_data_list.append(simulation_data)
    return sim_data_list

def load_task_pool(alpha_list, limit_of_children_simulations, limit_of_multi_simulations):
    '''
    Input:
        alpha_list : list of (alpha, decay) tuples
        limit_of_multi_simulations : number of children simulation in a multi-simulation
        limit_of_multi_simulations : number of simultaneous multi-simulations
    Output:
        task : [10 * (alpha, decay)] for a multi-simulation
        pool : [10 * [10 * (alpha, decay)]] for simultaneous multi-simulations
        pools : [[10 * [10 * (alpha, decay)]]]

    '''
    tasks = [alpha_list[i:i + limit_of_children_simulations] for i in range(0, len(alpha_list), limit_of_children_simulations)]
    pools = [tasks[i:i + limit_of_multi_simulations] for i in range(0, len(tasks), limit_of_multi_simulations)]
    return pools


# some other factory for other operators
def vector_factory(op, field):
    output = []
    vectors = ["cap"]
    
    for vector in vectors:
    
        alpha = "%s(%s, %s)"%(op, field, vector)
        output.append(alpha)
    
    return output
 
def ts_comp_factory(op, field, factor, paras):
    output = []
    #l1, l2 = [3, 5, 10, 20, 60, 120, 240], paras
    l1, l2 = [5, 22, 66, 240], paras
    comb = list(product(l1, l2))
    
    for day,para in comb:
        
        if type(para) == float:
            alpha = "%s(%s, %d, %s=%.1f)"%(op, field, day, factor, para)
        elif type(para) == int:
            alpha = "%s(%s, %d, %s=%d)"%(op, field, day, factor, para)
            
        output.append(alpha)
    
    return output
 
def twin_field_factory(op, field, fields):
    
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 240]
    outset = list(set(fields) - set([field]))
    
    for day in days:
        for counterpart in outset:
            alpha = "%s(%s, %s, %d)"%(op, field, counterpart, day)
            output.append(alpha)
    
    return output
 
def login_hk():
    
    username = ""
    password = ""
    
    # Create a session to persistently store the headers
    s = requests.Session()
    
    # Save credentials into session
    s.auth = (username, password)
    
    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    
    if response.status_code == requests.codes.unauthorized:
        # Check if biometrics is required
        if response.headers.get("WWW-Authenticate") == "persona":
            print(
                "Complete biometrics authentication by scanning your face. Follow the link: \n"
                + urljoin(response.url, response.headers["Location"]) + "\n"
            )
            input("Press any key after you complete the biometrics authentication.")
            
            # Retry the authentication after biometrics
            biometrics_response = s.post(urljoin(response.url, response.headers["Location"]))
            
            while biometrics_response.status_code != 201:
                input("Biometrics authentication is not complete. Please try again and press any key when completed.")
                biometrics_response = s.post(urljoin(response.url, response.headers["Location"]))
                
            print("Biometrics authentication completed.")
        else:
            print("\nIncorrect username or password. Please check your credentials.\n")
    else:
        print("Logged in successfully.")
    
    return s 

 

 


def filter_alpha(tracker,keys,return_type):
    
    if return_type == 'tracker':
        def contains_keyword(sublist, keys):
            return any(key in " ".join(map(str, sublist)) for key in keys)
        tracker['next'] = [item for item in tracker['next'] if not contains_keyword(item, keys)]
        tracker['decay'] = [item for item in tracker['decay'] if not contains_keyword(item, keys)]
        return tracker
    elif return_type == 'expression':
        original_list = tracker['next'] + tracker['decay']
        expression_list = [item[1] for item in original_list]
        result = [expr for expr in expression_list if not any(key in expr for key in keys)]
        return result
    
def get_alpha_pnl(session, alpha_id):
    count = 0
    while True:
        if count>30:
            s = login()
            count = 0
        pnl = session.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/pnl")
        retry_after = pnl.headers.get("Retry-After")
        if retry_after:
            # print(f"Sleeping for {retry_after} seconds")
            sleep(float(retry_after))
            # sleep(10)
        else:
            # print(f"{alpha_id} PnL retrieved")
            count += 1
            return (pnl, alpha_id)
    
def fetch_pnls(session, alpha_list):    
    pnl_ls = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Create a list of tasks
        futures = [executor.submit(get_alpha_pnl, session, alpha_id) for alpha_id in alpha_list]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            result = future.result()
            pnl_ls.append(result)
    return pnl_ls

def get_pnl_panel(session, alpha_list):
    alpha_pnls = fetch_pnls(session, alpha_list)
    pnl_df = pd.DataFrame()
    
    for pnl, alpha_id in tqdm(alpha_pnls):
        # 检查pnl对象是否有json方法，如果有，则调用该方法获取数据
        if hasattr(pnl, 'json') and callable(pnl.json):
            data = pnl.json()
        else:
            # 假设pnl已经是字典格式
            data = pnl

        # 检查records的列数
        if len(data['records'][0]) == 2:
            df = pd.DataFrame(data['records'], columns=['Date', alpha_id])
            df.set_index('Date', inplace=True)
        elif len(data['records'][0]) == 3:
            properties = data['schema']['properties']
            # 如果含有'risk-neutralized-pnl'，则保留这一列，并删除其他额外的列
            if any(prop['name'] == 'risk-neutralized-pnl' for prop in properties):
                records = [record[:2] for record in data['records']]
                df = pd.DataFrame(records, columns=['Date', alpha_id])
                df.set_index('Date', inplace=True)
            else:
                # 如果records的列数为3，但不包含'risk-neutralized-pnl'，则跳过这个alpha_id
                continue

        # 将当前alpha_id的DataFrame与总的DataFrame合并
        if pnl_df.empty:
            pnl_df = df
        else:
            pnl_df = pd.merge(pnl_df, df, on='Date', how='outer')
            pnl_df = pnl_df.sort_index()
    return pnl_df

def check_remove_low_pnl(
    s,
    bar,
    ids
):
    is_pnl_df = get_pnl_panel(s,ids)
    df_diff = is_pnl_df.diff(axis=0)
    df_processed = df_diff.apply(lambda x: x.apply(lambda y: 1 if abs(y) > 1e-10 else 0))
    non_zero_counts = df_processed.sum(axis=0)
    total_counts = df_processed.count(axis=0)
    percentages = non_zero_counts / total_counts

    ids_to_keep = percentages[percentages > bar].index.tolist()
    ids_to_remove = [id for id in ids if id not in ids_to_keep]
    # 打印结果
    print('KEEP:{',len(ids_to_keep),'个} ',ids_to_keep)
    print('REMOVE:{',len(ids_to_remove),'个} ',ids_to_remove)
    return ids_to_keep,is_pnl_df

def get_n_os_alphas(session, total_alphas, limit=100, max_retries=10,type='REGULAR'):
    fetched_alphas = []
    offset = 0
    retries = 0

    while len(fetched_alphas) < total_alphas and retries < max_retries:
        try:
            response = session.get(
                f"https://api.worldquantbrain.com/users/self/alphas?stage=OS&limit={limit}&offset={offset}"
            )
            response.raise_for_status()
            alphas = response.json()["results"]
            regular_items = [item for item in alphas if item.get('type') == 'REGULAR']
            super_items = [item for item in alphas if item.get('type') == 'SUPER']
            if type == 'REGULAR':
                fetched_alphas.extend(regular_items)
            elif type == 'SUPER':
                fetched_alphas.extend(super_items)
            if len(alphas) < limit:
                break
            offset += limit
            retries = 0
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}, retrying in {2 ** retries} seconds...")
            time.sleep(2 ** retries)  # 指数退避策略
            retries += 1  # 增加重试次数
            continue  # 继续下一次循环，不增加offset
        except Exception as err:
            print(f"An error occurred: {err}")
            break

    return fetched_alphas[:total_alphas]
def get_submitted_all(
    s,
    max: int = 300,
    addtional_id = [], #(用于包含一些准备提交但尚未提交的alphas）
    type = 'REGULAR',
    mode = 'ADD'
):  
    os_alpha_list = get_n_os_alphas(s, max,type = type)
    os_alpha_ids = [item['id'] for item in os_alpha_list]
    if mode == 'ALL':
        os_alpha_ids.extend(addtional_id)
        os_pnl_df = get_pnl_panel(s,os_alpha_ids)
        os_ret_df = os_pnl_df.pct_change(axis=0)
    else:
        #读取历史已保存pnl
        if type == 'REGULAR':
            file_name_MH = f'latest_regular_pnl_MH.csv'
        else:
            file_name_MH = f'latest_super_pnl_MH.csv'
        #这里只是一个拼装存储路径的函数，根据你自己的情况修改
        file_path_1 = get_file_path(file_name_MH,type)
        history_ret_df= pd.read_csv(file_path_1,index_col='Date')
        add_ids = [id for id in os_alpha_ids if id not in history_ret_df.columns]
        if(len(add_ids)>0):
            add_pnl_df = get_pnl_panel(s,add_ids)
            add_ret_df = add_pnl_df.pct_change(axis=0)
            os_ret_df = pd.concat([history_ret_df, add_ret_df], axis=1, ignore_index=False)
            #更新csv
            os_ret_df.to_csv(file_path_1, index=True, sep=',', encoding='utf-8')                   
            print(' '.join(add_ids) + ' has added')
            print(f'This account has: {len(update_ret_df.columns)}')
        else:
            os_ret_df = history_ret_df
            print('no new ID added')

    return os_ret_df

def gold_mining(s,is_ret_df, os_ret_df,type = 'REGULAR'):
   
    is_df = is_ret_df[(pd.to_datetime(is_ret_df.index) > pd.to_datetime(is_ret_df.index).max() - pd.DateOffset(years=4))]
    os_df = os_ret_df[(pd.to_datetime(os_ret_df.index) > pd.to_datetime(os_ret_df.index).max() - pd.DateOffset(years=4))]
    
    is_df = is_df.replace(0, np.nan)
    os_df = os_df.replace(0, np.nan)

    gold_ids = []
    for col_is in is_df.columns:
        ret = is_df[col_is]
        ret = pd.concat([ret,os_df],axis=1)
        corr_=ret.corr()
        cor_max = corr_.iloc[0,1:].max()
        if cor_max<0.7:
            gold_ids.append(col_is)
        else:
            if np.isnan(cor_max):
                cor_max = 0
                set_alpha_properties(s,col_is, name= 'NO_DATA', selection_desc= cor_max, tags=['MOVE'])   
            else:
                if type == 'REGULAR':
                    set_alpha_properties(s,col_is, name= col_is, selection_desc= cor_max, tags=['Self Correlation']) 
                else:
                    set_alpha_properties(s,col_is, name= cor_max, selection_desc= cor_max, tags=['Self Correlation'])  
    print(gold_ids)
    return gold_ids

def check_remove_self_correlation(
    s,
    ids,
    is_pnl_df,
    os_ret_df
):
    # is_pnl_df = get_pnl_panel(s,ids)
    is_ret_df = is_pnl_df - is_pnl_df.ffill().shift(1)
    pass_is_ids = gold_mining(s,is_ret_df, os_ret_df)
    return pass_is_ids

 

def get_file_path(file_name, function_code):
    # 根据 function_code 确定子目录名称
    if function_code == 'REGULAR':
        sub_dir = 'regular'
    else:
        sub_dir = 'super'
    
    # 构建基础路径（假设基础路径为项目下的 pnl_data 目录）
    base_dir = os.path.join('pnl_data', sub_dir)
    
    # 确保目录存在
    os.makedirs(base_dir, exist_ok=True)
    
    # 返回完整的文件路径
    return os.path.join(base_dir, file_name)


def pc_check(s, alpha_id):
    #  https://api.worldquantbrain.com/competitions/IQC2025S1/alphas/xrgpKXN/before-and-after-performance
    while True:
        result = s.get("https://api.worldquantbrain.com/competitions/IQC2025S1/alphas/" + alpha_id + "/before-and-after-performance")
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    try:
        if result.json().get("score", 0) == 0:
            print("logged out")
            return "sleep"
        before = result.json()["score"]["before"]
        after = result.json()["score"]["after"]
        add = after - before
        return(alpha_id,add)
        # if add > 300:
        #     print(alpha_id,add)
        #     return alpha_id,add
    except:
        print("catch: %s"%(alpha_id))
        return "error"
    
