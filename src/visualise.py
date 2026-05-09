from matplotlib import pyplot as plt
from pathlib import Path
import statistics
import logging

plt.style.use('seaborn-v0_8')
logger = logging.getLogger(__name__)

def get_slug(question):
    """Sanitize a question string into a filesystem-safe slug
    
    Converts the question to lowercase and replaces spaces with underscores, while stripping characters that are reserved or illegal in filenames

    Args:
        question(str): The natural language question to be converted.

    Returns:
        str: A sanitized, lowercase string suitable for use as a filename or URL component.
    """
    slug=question.lower().replace(" ","_")
    for char in ["?", "/", "\\", ":", "*", "<", ">", "|", "\""]:
        slug=slug.replace(char,"")
    return slug
    

def save_plot(run_id,question):
    """Save the current matplotlib figure to the local filesystem and close the matplotlib figure.

    Args:
        run_id (int): The unique identifier for the current pipeline execution.
        question (str): The natural language question to be used in plotname.
    """
    slug=get_slug(question)
    plot_name=str(run_id)+"_"+slug
    
    file_path=Path(f"outputs/graphs/{plot_name}.png")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(file_path)
    plt.close()
    

def create_bar_chart(run_id,question,answer):
    """Generate and save a bar chart high-cardinality categorical data.
    
    Visualizes a dictionary of results where keys represent categories and values represent numerical counts.

    Args:
        run_id (int): The unique identifier for the current pipeline execution.
        question (str): The natural language question to be used in plotname.
        answer (dict): A dictionary mapping categorical labels (str) 
            to numerical values (int/float).
    """
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
    """Generate and save a pie chart for low-cardinality categorical data.
    
    Visualizes a dictionary of results where keys represent categories and values represent numerical counts.

    Args:
        run_id (int): The unique identifier for the current pipeline execution.
        question (str): The natural language question to be used in plotname.
        answer (dict): A dictionary mapping categorical labels (str) to numerical values (int/float).
    """
    slices=[]
    labels=[]
    for category,value in answer.items():
        labels.append(category)
        slices.append(value)
        
    plt.pie(slices,labels=labels,shadow=True,autopct='%1.1f%%',wedgeprops={'edgecolor': 'black'})
    plt.title(question)
    
    save_plot(run_id,question)
    
def create_histogram(run_id,question,answer):
    """Generate and save a histogram for list of numerical data.
    
    Visualize a frequency distribution of the provided numerical list using logarithmic scaling and identifies the central tendency by overlaying a median line.

    Args:
        run_id (int): The unique identifier for the current pipeline execution.
        question (str): The natural language question to be used in plotname.
        answer (list): A list of numerical values (int or float) to be plotted
    """
    plt.hist(answer,bins=20,edgecolor='black',log=True)
    plt.title(question)
    plt.xlabel("Category")
    plt.ylabel("Frequency")
    
    median_value=statistics.median(answer)
    plt.axvline(median_value, color='#fc4f30', label='Median', linewidth=3)
    plt.legend()

    plt.tight_layout()
    
    save_plot(run_id,question)
    
def create_line_chart(run_id,question,answer):
    """Generate and save a line chart to visualize trends over time.
    
    Automatically identifies temporal and numerical keys within a list of  dictionaries to plot sequential data.

    Args:
        run_id (int): The unique identifier for the current pipeline execution.
        question (str): The natural language question to be used in plotname.
        answer (list[dict]): A list of dictionaries containing the records to be plotted.
    """
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
    """Orchestrate data visualization by mapping query results to graph types.
    
    Analyzes the structure and data types of the execution results to automatically trigger the appropriate plotting function (Like - Pie, Bar, Line, or Histogram) based on cardinality and value types.

    Args:
        run_id (int): The unique identifier for the current pipeline execution.
        analysis_result (dict): Mapping of questions to their respective database results (can be lists of records or metric dictionaries).
    """
    for question in analysis_result.keys():
        answer=analysis_result[question]
        if answer :
            if isinstance(answer,dict):
                graph=None
                for key,value in answer.items():
                    if isinstance(key,str) and isinstance(value,int):
                        if len(answer) <= 7 :
                            graph="pie_chart" 
                        else:
                            graph="bar_chart"            
                if graph=="bar_chart":
                    create_bar_chart(run_id,question,answer)
                elif graph=="pie_chart":
                    create_pie_graph(run_id,question,answer)
                
            if isinstance(answer,list):
                if all(isinstance(num,(int,float)) for num in answer):
                    create_histogram(run_id,question,answer) 
                else:
                    graph=None
                    item=answer[0]
                    if (isinstance(item,dict)):
                        for key in item.keys():
                            if isinstance(key,str) and ("date" in key.lower() or "time" in key.lower()):
                                graph="line_chart"
                                        
                    if graph=="line_chart":
                        create_line_chart(run_id,question,answer)
                    else:
                        # it's a list of dicts with categorical string keys - bar chart/pie chart
                        temp={}
                        for dictionary in answer:
                            values = list(dictionary.values())
                            label = values[0]
                            number = values[1]
                            temp[label]=number
                        
                        for key,value in temp.items():
                            if isinstance(key,str) and (isinstance(value,int) or isinstance(value,float)):
                                if len(answer) <= 7 :
                                    graph="pie_chart" 
                                else:
                                    graph="bar_chart"  
                                            
                    if graph=="bar_chart":
                        create_bar_chart(run_id,question,temp)
                    elif graph=="pie_chart":
                        create_pie_graph(run_id,question,temp)
        else:
            logger.error("Answer list is empty")