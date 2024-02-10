#%% 匯入套件(包含我們寫好的function檔)
import pandas as pd 
import streamlit as st
import plotly.io as pio
pio.renderers.default='browser'
import numpy as np
import plotly.graph_objects as go
#from github import Github
from function import *
from itertools import product


st.set_page_config(
    page_title='光服務數據中心',
    layout='wide',
    initial_sidebar_state='expanded'
    )
#設定style
st.markdown(
    """
    <style>
        .stTextInput > label {
            font-size:105%; 
            font-weight:bold; 
            color:blue;
        }
        .stMultiSelect > label {
            font-size:105%; 
            font-weight:bold; 
            color:blue;
        } 
        .stRadio > label {
            font-size:105%; 
            font-weight:bold; 
            color:blue;
        } 
        .stSlider > label {
            font-size:105%; 
            font-weight:bold; 
            color:blue;
        }
    </style>
    """, 
    unsafe_allow_html=True)

table_style = """
<style>
table {
    font-size: 9px;
}
</style>

"""
expander_style = """
<style>
expander {
    font-size: 9px;
    color:blue;
}
</style>
"""



def project_search():

    # today,yesterday,this_week_start,this_week_end,last_week_start,last_week_end,this_month_start,this_month_end,last_month_start,last_month_end = get_date_data()
    df_file = pd.read_csv("data/df_file.csv",encoding="utf-8-sig")  # 檔案資訊
    df_light,filename_light, upload_date_light = upload(df_file,"light",None)  # id <==> coordinate
    df_coor,filename_coor, upload_date_coor = upload(df_file,"coor",None)  # coordinate <==> scenes
    df_arobjs,filename_arobjs, upload_date_arobjs = upload(df_file,"arobjs",None)
    df_scan_coor_scene_city,df_coor_city,df_coor,df_arobjs = get_scan_data(df_light,df_coor,df_arobjs)
    
    df_click_lig = pd.read_csv("data/obj_click_log.csv",encoding="utf-8-sig") 
    df_click_lig['time'] = pd.to_datetime(df_click_lig['time'], format='%Y年%m月%d日 %H:%M', errors='coerce')
    lastest_click_date = df_click_lig['time'].max()
    df_click_lig['formatted_time'] = df_click_lig['time'].dt.strftime('%Y-%m-%d %H:%M')
    df_click_lig['formatted_time'] = pd.to_datetime(df_click_lig['formatted_time'])
    df_click_lig = df_click_lig.sort_values(by='time', ascending=True)
    df_click_lig['obj_id_revised'] = df_click_lig['ar_object_id'].astype(str).str[:5]
    df_arobjs['obj_id'] = df_arobjs['obj_id'].astype(str) #資訊球點擊會出現包含點擊位置的數
    df_click_lig_objs = df_click_lig.merge(df_arobjs,left_on='obj_id_revised', right_on='obj_id',how='inner')

    
    lastest_update_date = df_scan_coor_scene_city['scantime'].max()
    col_t1, col_t2 = st.columns(2)
    col_t1.markdown(f'<span style="color:red">掃描數據更新時間：{lastest_update_date}</span>', unsafe_allow_html=True)
    col_t2.markdown(f'<span style="color:red">點擊數據更新時間：{lastest_click_date}</span>', unsafe_allow_html=True)
    st.markdown("<h4 style='text-align: center; background-color: #4f9ac3; padding: 10px;'>數據查詢平台</h4>", unsafe_allow_html=True)
    st.markdown("<h6 style='text-align: left ; background-color: #96c5df; padding: 10px;'>查詢參數設定</h6>", unsafe_allow_html=True)

    now = datetime.now()
    default_start_date = now - timedelta(days=7)
    
    col1,col2,col3 = st.columns([1,1,2])
    start_date = col1.date_input("開始日期", value=default_start_date, max_value = datetime.today())
    end_date = col2.date_input("結束日期", value=now, max_value = datetime.today())  
    
    filtered_date_df = date_filter(df_scan_coor_scene_city,start_date,end_date+timedelta(days=1))
    field_count = filtered_date_df['field'].value_counts()

    # 將計數結果轉換為 DataFrame 以便於顯示
    field_count_df = field_count.reset_index()
    field_count_df.columns = ['場域', '掃描量']
    field_count_df.set_index('場域', inplace=True)
    selected_field = col3.selectbox(label="選擇場域", options=field_count_df.index)
    # 根據計數進行降序排序
    field_count_df = field_count_df.sort_values(by='掃描量', ascending=False)
    df_coor_city_filtered = df_coor_city[df_coor_city['field'] == selected_field]
    selected_coor_list = df_coor_city_filtered['coor_name'].tolist()
    df_coor_filtered = df_coor[df_coor['coor_name'].isin(selected_coor_list)]
    selected_scene_list = df_coor_filtered['scene_name'].tolist()

    df_click_lig_objs_sorted = df_click_lig_objs.sort_values(by='time').reset_index(drop=True) 
    filtered_df_click_lig_objs = df_click_lig_objs_sorted[(df_click_lig_objs_sorted['time'].dt.date >= start_date) & (df_click_lig_objs_sorted['time'].dt.date <= end_date)]
    
    # 使用groupby對'obj_scene'進行分組，然後計算每組的數量
    obj_click_count = filtered_df_click_lig_objs.groupby('obj_scene')['obj_id_revised'].count().reset_index(name='counts')
    obj_click_count_sorted = obj_click_count.sort_values(by='counts', ascending=False)
    selected_secnes = st.multiselect(label='選擇查詢場景Scene',options=obj_click_count['obj_scene'].sort_values())

    # 以 'field' 字段為例，我們將按照此字段進行計數並排序
    # 首先，對 'field' 字段的值進行計數
    with st.expander('點擊看場域掃描量數據'):
        st.info('場域掃描量')
        st.dataframe(field_count_df.transpose(),use_container_width = True)

    with st.expander("點擊查看物件點擊排行榜 By 場景"):
        fig_lig_click_rank = go.Figure()
        fig_lig_click_rank.add_trace(go.Bar(
                    y=obj_click_count_sorted['obj_scene'], 
                    x=obj_click_count_sorted['counts'], 
                    text=obj_click_count_sorted['counts'],
                    orientation='h',  # 這會使柱狀圖水平顯示                
        ))

        fig_lig_click_rank.update_layout(
            title={
                'text': f"場景 vs 點擊量({start_date}~{end_date})",
                'x': 0.5,
                'xanchor': 'center'
            },
            xaxis_title="點擊量",
            yaxis_title="場景名稱",
            yaxis={
                'autorange': 'reversed',
                'tickangle': 0,  # 旋轉y軸的標籤
                'tickfont': {'size': 10},   # 調整標籤字體大小
            },
        )
        fig_lig_click_rank.update_layout(
            height=1000,
        )
        st.plotly_chart(fig_lig_click_rank)

    fields = sorted(filtered_date_df['field'].dropna().unique().tolist())
    scenes = sorted(filtered_date_df['scene_name'].dropna().unique().tolist())

    fieldcheck_df = df_coor_city[df_coor_city['field']==selected_field]

    app = fieldcheck_df['app'].unique()[0]
    property_id = fieldcheck_df['property_id'].unique()[0]

    
    scenes_list = get_scenes(filtered_date_df,selected_field)
    scenes_list_table = scenes_list.rename(columns={'lig_id':'光標籤id','coor_name':'坐標系','scene_name':'場景'})
    scenes_list_table.set_index("光標籤id", inplace=True)
    
    with st.expander(f'點擊查看「{selected_field}」坐標系'):
        st.dataframe(scenes_list_table,width=None)

    
    merged_df = pd.DataFrame()
    for lig_id in scenes_list['lig_id']:
        con = filtered_date_df['lig_id'] == lig_id
        df_ids = filtered_date_df[con]
        merged_df = pd.concat([merged_df, df_ids], ignore_index=True)
    st.markdown("<h6 style='text-align: left ; background-color: #96c5df; padding: 10px;'>掃描數據</h6>", unsafe_allow_html=True)
    st.write(f"總掃描次數 : {len(merged_df)}")
  
    date_range = pd.date_range(start=start_date, end=end_date)    
    avg = round(len(merged_df)/len(date_range), 2)
    st.write(f"平均每日掃描次數: {avg}/日")
    
    merged_df['scan_date'] = merged_df['scantime'].dt.date
    merged_df['scan_date'] = pd.to_datetime(merged_df['scan_date'])
    id_to_coor = dict(zip(scenes_list['lig_id'], scenes_list['coor_name']))
    combinations = list(product(scenes_list['lig_id'], date_range))
    all_combinations_data = [(lig_id, date, id_to_coor[lig_id]) for lig_id, date in combinations]
    all_combinations = pd.DataFrame(all_combinations_data, columns=['lig_id', 'scan_date', 'coor_name'])
    scan_counts = merged_df.groupby(['lig_id', 'coor_name','scan_date']).size().reset_index(name='scan_count')
    scan_counts = pd.merge(all_combinations, scan_counts, on=['lig_id','coor_name', 'scan_date'], how='left').fillna(0)
    pivot_df = scan_counts.pivot_table(index='scan_date', columns=['coor_name','lig_id'], values='scan_count', aggfunc='sum')
    pivot_fig_df = scan_counts.pivot_table(index='scan_date', columns=['lig_id'], values='scan_count', aggfunc='sum')

    y1_max = pivot_fig_df.sum(axis=1).max()
    # 計算最接近且大於 y1_max 的 50 的倍數
    y1_ceiling = (y1_max // 200 + 1) * 200
    y1_interval = y1_ceiling / 10
    y1_tickvals = [i*y1_interval for i in range(11)]

    # st.write(y1_max,max_tick,y1_tick_val)
    cumulative_sum = pivot_fig_df.sum(axis=1).cumsum() 
    y2_max = cumulative_sum.max()
    # 計算最接近且大於 y2_max 的 50 的倍數
    y2_ceiling = (y2_max // 250 + 1) * 250
    y2_interval = y2_ceiling / 10
    y2_tickvals = [i*y2_interval for i in range(11)]
    
    pivot_fig_df.index = pivot_fig_df.index.strftime('%-m/%-d %a')
    pivot_df_T = pivot_df.transpose()
    fig = go.Figure()
    for id in scenes_list['lig_id']:
        coor_name = scenes_list.loc[scenes_list['lig_id'] == id, 'coor_name'].iloc[0]
        fig.add_trace(go.Bar(
            x=pivot_fig_df.index,
            y=pivot_fig_df[id],
            text=pivot_fig_df[id],
            name= f"{id}:{coor_name}",
        ))      
    total_sum = pivot_fig_df.sum(axis=1)  # 計算每列的總和

    fig.add_trace(go.Scatter(
        x=pivot_fig_df.index,
        y=total_sum,
        mode='lines+markers+text',  # 這裡組合了線、標記和文字
        text=total_sum,
        textposition='top center',  # 設定文字位置
        textfont=dict(size=10),
        name='當日總和',
        line=dict(color='red'),  # 設定線條顏色
        yaxis='y1'
    ))
    fig.add_trace(go.Scatter(
        x=pivot_fig_df.index,
        y=cumulative_sum,
        mode='lines+markers+text',  # 這裡組合了線、標記和文字
        text=cumulative_sum,
        textposition='top center',  # 設定文字位置
        textfont=dict(size=10),
        name='累計掃描量',
        line=dict(color='blue'),  # 設定線條顏色
        yaxis='y2'
    ))

    fig.update_layout(        
        title={
            'text': f"{selected_field}掃描量({start_date}~{end_date})",
            'x': 0.5,
            'xanchor': 'center'
        },        
        xaxis_title="時間",
        yaxis_title="掃描量",
        xaxis={
            'tickangle':90, 
            'tickformat':'%-m/%-d %a',
            'type': 'category'
        },
        yaxis=dict(
            title="掃描量",
            range=[0, y1_ceiling], 
            tickvals=y1_tickvals,  # 使用計算的刻度值
            tickfont=dict(color="red")
        ),
        yaxis2=dict(
            title="累計掃描量",
            range=[0, y2_ceiling], 
            tickvals=y2_tickvals,  # 使用計算的刻度值
            titlefont=dict(color="blue"),
            tickfont=dict(color="blue"),
            overlaying='y',
            side='right',
        ),        
        legend=dict(
            orientation="h",  # horizontal orientation
            yanchor="bottom",
            y=-0.4,
            xanchor="right",
            x=1
        ),
        width=1000,
        height=600
    )
    st.plotly_chart(fig)    

    csv = csv_download(pivot_df_T)
    st.download_button(
     label = "下載掃描數據csv檔",
     data = csv,
     file_name='場域掃描數據.csv',
     mime='text/csv',
     )
    

    st.markdown("<h6 style='text-align: left ; background-color: #96c5df; padding: 10px;'>點擊數據</h6>", unsafe_allow_html=True)
    
    df_arobjs['obj_id'] = df_arobjs['obj_id'].astype(int)
    st.write(f'使用的APP：{app}')
    st.write(f'property_id:{property_id}')
    scenes = sorted(obj_click_count['obj_scene'].to_list())
    filter_raw_data = filtered_df_click_lig_objs[filtered_df_click_lig_objs['obj_scene'].isin(selected_secnes)]

    daily_counts = filter_raw_data.groupby([filter_raw_data['formatted_time'].dt.date, 'obj_scene']).size().unstack(fill_value=0)
    date_range = pd.date_range(start=start_date, end=end_date)
    daily_counts = daily_counts.reindex(date_range, fill_value=0)
    daily_counts['總和'] = daily_counts.sum(axis=1)
    daily_counts['累計點擊數'] = daily_counts.iloc[:, :-1].sum(axis=1).cumsum()
    daily_counts['掃描量'] = total_sum.values
    daily_counts['平均點擊率'] =  (daily_counts['總和'] / daily_counts['掃描量']).round(1)
    cols = daily_counts.columns.tolist()
    daily_counts = daily_counts[cols]
    daily_counts.index = daily_counts.index.strftime('%-m/%-d %a')

    y1_max = daily_counts['總和'].max()
    # 計算最接近且大於 y1_max 的 50 的倍數
    y1_ceiling = (y1_max // 200 + 1) * 200
    y1_interval = y1_ceiling / 10
    y1_tickvals = [i*y1_interval for i in range(11)]

    y2_max = daily_counts['累計點擊數'].max()
    # 計算最接近且大於 y2_max 的 50 的倍數
    y2_ceiling = (y2_max // 250 + 1) * 250
    y2_interval = y2_ceiling / 10
    y2_tickvals = [i*y2_interval for i in range(11)]

    fig_histogram = go.Figure()
    for scene in daily_counts.columns[:-4]:  # 不包括 "總和"
        fig_histogram.add_trace(go.Bar(
            x=daily_counts.index,
            y=daily_counts[scene],
            text = daily_counts[scene],
            name=scene
        ))

    fig_histogram.add_trace(go.Scatter(
        x=daily_counts.index,
        y=daily_counts['總和'],
        text= daily_counts['總和'],
        mode='lines+markers+text',
        textposition='top center',  # 設定文字位置
        textfont=dict(size=10),
        line=dict(color='red', width=2),
        name='總和點擊數',
        yaxis='y1'
    ))

    fig_histogram.add_trace(go.Scatter(
        x=daily_counts.index,
        y=daily_counts['累計點擊數'],
        text= daily_counts['累計點擊數'],
        mode='lines+text',
        textposition='top center',
        textfont=dict(size=10),
        line=dict(color='blue', width=2),
        name='累計點擊數',
        yaxis='y2'
    ))

    fig_histogram.update_layout(
        barmode='stack',
        title={
            'text': f"{selected_field}點擊量({start_date}~{end_date})",
            'x': 0.5,
            'xanchor': 'center'
        },  

        xaxis_title='日期',
        yaxis_title='物件點擊數量',
        xaxis={
            'tickangle':90, 
            'tickformat':'%-m/%-d %a',
            'type': 'category'
        },
        yaxis=dict(
            title="點擊量",
            range=[0, y1_ceiling], 
            tickvals=y1_tickvals,  # 使用計算的刻度值
            tickfont=dict(color="red")
        ),
        yaxis2=dict(
            title="累計點擊量",
            range=[0, y2_ceiling], 
            tickvals=y2_tickvals,  # 使用計算的刻度值
            titlefont=dict(color="blue"),
            tickfont=dict(color="blue"),
            overlaying='y',
            side='right',
        ),  

        legend=dict(
            orientation="h",  # horizontal orientation
            yanchor="bottom",
            y=-0.4,
            xanchor="right",
            x=1
        ),

        width = 1200,
        height = 800
    )

    # 顯示圖表
    st.plotly_chart(fig_histogram)

    filtered_data_without_duplicates = filter_raw_data.drop_duplicates(subset='time', keep=False)

    sorted_df_click_lig_objs = filtered_df_click_lig_objs.sort_values(by="time", ascending=False)
    filtered_df_click_lig_objs = sorted_df_click_lig_objs[sorted_df_click_lig_objs['obj_scene'].isin(selected_secnes)]
    scan_counts = filtered_df_click_lig_objs.groupby(['obj_id', 'obj_name','obj_scene']).size().reset_index(name='點擊量')
    sorted_scan_counts = scan_counts.sort_values(by='點擊量', ascending=False)
    sorted_scan_counts['id_name_combined'] = sorted_scan_counts['obj_id'] + '：' + sorted_scan_counts['obj_name']

    with st.expander("點擊查看物件點擊排行榜"):


        fig_lig_click = go.Figure()
        fig_lig_click.add_trace(go.Bar(
                    y=sorted_scan_counts['id_name_combined'].head(25), 
                    x=sorted_scan_counts['點擊量'].head(25), 
                    text=sorted_scan_counts['點擊量'].head(25),
                    orientation='h',  # 這會使柱狀圖水平顯示                
        ))

        fig_lig_click.update_layout(
            title={
                'text': f"物件名稱 vs 點擊量({start_date}~{end_date})",
                'x': 0.5,
                'xanchor': 'center'
            },
            xaxis_title="點擊量",
            yaxis_title="物件名稱",
            yaxis={
                'autorange': 'reversed',
                'tickangle': 0,  # 旋轉y軸的標籤
                'tickfont': {'size': 10},   # 調整標籤字體大小
            },
        )
        fig_lig_click.update_layout(
            height=1000,
        )
        st.plotly_chart(fig_lig_click)

        csv_sorted_scan_counts = csv_download(sorted_scan_counts)
        st.download_button(
        label = "點擊csv檔",
        data = csv_sorted_scan_counts,
        file_name='點擊.csv',
        mime='text/csv',
        )   

def parameters():
    df_file = pd.read_csv("data/df_file.csv",encoding="utf-8-sig")  # 檔案資訊
    # 建立一個下拉式選單供使用者選擇分類
    
    st.write(df_file)
    category = st.selectbox('請選擇上傳分類：', df_file['db'].unique())

    selected_filename = df_file[df_file['db'] == category]['filename'].values[0] 
    filepath = os.path.join("data", selected_filename)
    modified_date = pd.to_datetime(os.path.getmtime(filepath), unit='s')
    uploaded_file = st.file_uploader(f"上傳 {category} 檔案:檔案位置{filepath} 最新修改日期：{modified_date}", type=['csv'])

    if category=='scan'and uploaded_file:
        original_df = pd.read_csv("data/scandata_new.csv", encoding='utf-8-sig')
        uploaded_df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        uploaded_df['Scan time'] = pd.to_datetime(uploaded_df['Scan time'], format='%Y年%m月%d日 %H:%M', errors='coerce')
        uploaded_df['Scan time'] = uploaded_df['Scan time'].apply(lambda x: x.strftime('%Y/%-m/%-d %H:%M'))
        uploaded_df.rename(columns={'Scan time': 'Timestamp', 'Scan light': 'lig_id'}, inplace=True)
        merged_df = pd.concat([original_df, uploaded_df], ignore_index=True)
        merged_df.sort_values('Timestamp')
        merged_df.drop_duplicates(inplace=True)
        merged_df.to_csv("data/scandata_new.csv", encoding="utf-8-sig", index=False)
    elif category=='click'and uploaded_file:
        original_df = pd.read_csv("data/obj_click_log.csv", encoding='utf-8-sig')
        original_df['time'] = pd.to_datetime(original_df['time'], format='%Y年%m月%d日 %H:%M', errors='coerce')
        uploaded_df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        uploaded_df['Time'] = pd.to_datetime(uploaded_df['Time'], format='%Y年%m月%d日 %H:%M', errors='coerce')
        uploaded_df.rename(columns={'Time': 'time', 'Code name': 'code_name', 'Obj': 'ar_object_id'}, inplace=True)
        merged_df = pd.concat([original_df, uploaded_df], ignore_index=True)
        merged_df.sort_values('time')
        merged_df.drop_duplicates(inplace=True)
        st.write(merged_df)
        merged_df.to_csv("data/obj_click_log.csv", encoding="utf-8-sig", index=False)
    elif uploaded_file and category!='click' and category!='scan':
    # 若有檔案上傳，則讀取該檔案
        uploaded_df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        original_filename = uploaded_file.name
        save_path = os.path.join("data", original_filename)
        uploaded_df.to_csv(save_path, encoding="utf-8-sig", index=False)
        df_file.loc[df_file['db'] == category, 'filename'] = original_filename
        df_file.to_csv("data/df_file.csv", encoding="utf-8-sig", index=False)
        st.write(f"{category} 檔案已成功上傳並更新!")
        st.write(df_file)
    

    df_coor_city = pd.read_csv("data/coor_city.csv", encoding="utf-8-sig")
    coor_filename = df_file[df_file['db'] == 'coor']['filename'].values[0]
    df_coor = pd.read_csv(os.path.join("data", coor_filename), encoding="utf-8-sig")
    df_coor = df_coor.rename(columns={
    'Id': 'coor_id',
    'Name': 'coor_name',
    'Created at': 'coor_createtime',
    'Name [Scenes]': 'scene_name',
    })
    new_ids = set(df_coor['coor_id']) - set(df_coor_city['coor_id'])
    new_rows = df_coor[df_coor['coor_id'].isin(new_ids)][['coor_id', 'coor_name']]
    new_rows['city'] = None
    new_rows['field'] = None
    new_rows['app'] = None
    new_rows['property_id'] = None

    df_coor_city = pd.concat([df_coor_city, new_rows], ignore_index=True)

    for index, row in df_coor.iterrows():
        df_coor_city.loc[df_coor_city['coor_id'] == row['coor_id'], 'coor_name'] = row['coor_name']
    # 在最初創建一個占位符
    data_editor_placeholder = st.empty()
    df_coor_city = df_coor_city.sort_values(by='coor_id', ascending=False)
    edited_df = data_editor_placeholder.data_editor(df_coor_city)
    if st.button('更新'):
        edited_df.to_csv("data/coor_city.csv", encoding="utf-8-sig", index=False)
        st.write('更新成功!')
        df_coor_city = edited_df
        data_editor_placeholder.data_editor(df_coor_city)

#%% 頁面呈現 ============================================================================= #
def main():
    taipei_timezone = pytz.timezone('Asia/Taipei')
    datetime_taipei = datetime.now(taipei_timezone).date()
    today = datetime_taipei
    st.write(f"今天日期：{today}")

    # Check if 'page' is already in the session state
    if 'page' not in st.session_state:
        st.session_state.page = "project_search"

    # Sidebar with buttons
    if st.sidebar.button('專案查詢'):
        st.session_state.page = "project_search"
    if st.sidebar.button("參數設定"):
        st.session_state.page = 'parameters'
    
    # Load the content based on current page
    if st.session_state.page == "project_search":
        project_search()
    elif st.session_state.page == "parameters":
        parameters()

    

    
#%% Web App 測試 (檢視成果)  ============================================================================= ##    
if __name__ == "__main__":
    main()
    
