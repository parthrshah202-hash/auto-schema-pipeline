from matplotlib import pyplot as plt
from pathlib import Path
import statistics
import logging

plt.style.use('seaborn-v8')
logger = logging.getLogger(__name__)

def get_slug(question):
    slug=question.lower().replace(" ","_")
    for char in ["?", "/", "\\", ":", "*", "<", ">", "|", "\""]:
        slug=slug.replace(char,"")
    return slug
    

def save_plot(run_id,question):
    slug=get_slug(question)
    plot_name=str(run_id)+"_"+slug
    
    file_path=Path(f"outputs/graphs/{plot_name}.png")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(file_path)
    plt.close()
    

def create_bar_chart(run_id,question,answer):
    x_axis=[]
    y_axis=[]
    for x_vals,y_vals in answer.items():
        x_axis.append(x_vals)
        y_axis.append(y_vals)
    
    plt.bar(x_axis,y_axis)
    plt.title(question)
    plt.xlabel("Category")
    plt.ylabel("Values")
    plt.grid(True)
    plt.tight_layout()
    
    save_plot(run_id,question)
    
def create_pie_graph(run_id,question,answer):
    slices=[]
    labels=[]
    for category,value in answer.items():
        labels.append(category)
        slices.append(value)
        
    plt.pie(slices,labels=labels,shadow=True,autopct='%1.1f%%',wedgeprops={'edgecolor': 'black'})
    plt.title(question)
    
    save_plot(run_id,question)
    
def create_histogram(run_id,question,answer):
    plt.hist(answer,bins=20,edgecolor='black',log=True)
    plt.title(question)
    plt.xlabel("Category")
    plt.ylabel("Values")
    
    median_value=statistics.median(answer)
    plt.axvline(median_value, color='#fc4f30', label='Median', linewidth=3)
    plt.legend()

    plt.tight_layout()
    
    save_plot(run_id,question)
    
def create_line_chart(run_id,question,answer):
    item=answer[0]
    x_key=None
    y_key=None
    for key in item.keys():
        if "date" in key.lower() or "time" in key.lower():
            x_key=key
        else:
            y_key=key
            
    if not x_key:
        keys = list(item.keys())
        x_key = keys[0]
        y_key = keys[1]

    x_axis = [d[x_key] for d in answer]
    y_axis = [d[y_key] for d in answer]
        
    plt.plot(x_axis,y_axis)
    plt.title(question)
    plt.xlabel("Category")
    plt.ylabel("Values")
    plt.grid(True)
    plt.tight_layout()
    
    save_plot(run_id,question)
    

def map_graph_types(run_id,analysis_result):
    for question in analysis_result.keys():
        answer=analysis_result[question]
        if answer :
            if type(answer)==dict:
                graph="None"
                for key,value in answer.items():
                    key_type=type(key)
                    value_type=type(value)
                    if key_type==str and value_type==int:
                        if len(answer) <= 7 :
                            graph="pie_chart" 
                        else:
                            graph="bar_chart"            
                if graph=="bar_chart":
                    create_bar_chart(run_id,question,answer)
                elif graph=="pie_chart":
                    create_pie_graph(run_id,question,answer)
                
            if type(answer)==list:
                if all(isinstance(num,(int,float)) for num in answer):
                    create_histogram(run_id,question,answer) 
                
                
                graph="None"
                item=answer[0]
                if (isinstance(item,dict)):
                    for key in item.keys():
                        if isinstance(key,str) and ("date" in key.lower() or "time" in key.lower()):
                            graph="line_chart"
                                    
                if graph=="line_chart":
                    create_line_chart(run_id,question,answer)
        else:
            logger.error("Answer list is empty")