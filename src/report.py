from fpdf import FPDF
import json
from pathlib import Path
import visualise
import logging

logger=logging.getLogger(__name__)

title="Analysis Report"

class PDF(FPDF):
    '''
    def header(self):
        self.set_font('Arial', 'B', 22)
        self.set_fill_color(5, 5, 5)
        self.set_text_color(252, 250, 250)
        self.set_line_width(1)
        self.cell(0, 10, title, border=1, align='C', fill=True)
        self.set_text_color(0, 0, 0)
        self.set_line_width(0.2)

        
    def footer(self):
        # Set position of the footer
        self.set_y(-15)
        
        # set font
        self.set_font('Arial', 'I', 8)
        
        self.set_text_color(169,169,169) #grey
        
        # Page number
        self.cell(0, 10, f'Page {self.page_no()}', align='C')
    '''

def read_json(run_id):
    with open(f"outputs/json/results_{run_id}.json",'r',encoding='utf-8') as file:
        output_data=json.load(file)
        return output_data
    
def create_report(run_id):
    pdf=PDF()
    pdf.set_top_margin(30)
    pdf.set_auto_page_break(auto=True, margin=15)
    try:
        output_data=read_json(run_id)
        pdf.add_page()
        pdf.set_line_width(0.2)
        
        for key,val in output_data.items():
            if key=="file_data":
                pdf.set_font("Arial",size=18)
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(0,10,"Meta-Data of File",align='C')
                pdf.set_font("Arial",size=16)
                for metric,answer in val.items():
                    pdf.set_x(pdf.l_margin)
                    pdf.multi_cell(0,10,txt=f"{metric} : {answer}")
                pdf.ln(10)
            
            elif key=="file_info":
                pdf.set_font("Arial",size=18)
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(0,10,"Information of File",align='C')
                pdf.set_font("Arial",size=14)
                for metric,answer in val.items():
                    pdf.set_x(pdf.l_margin)
                    pdf.multi_cell(0,10,txt=f"{metric} : {answer}")
                pdf.ln(10)
                    
            elif key=="analysis_result":
                pdf.set_font("Arial",size=18)
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(0,10,"Result after AI Analysis",align='C')
                pdf.set_font("Arial",size=12)
                for question,answer in val.items():
                    pdf.set_x(pdf.l_margin)
                    
                    if isinstance(answer,(int,float,str)):
                        pdf.set_x(pdf.l_margin)
                        pdf.multi_cell(0,10,txt=f"Question : {question}")
                        pdf.set_x(pdf.l_margin)
                        pdf.multi_cell(0,10,txt=f"Answer : {answer}")
                        pdf.ln(5)
                    elif isinstance(answer,dict):
                        pdf.set_x(pdf.l_margin)
                        pdf.multi_cell(0,10,txt=f"Question : {question}")
                        for q,a in answer.items():
                            pdf.set_x(pdf.l_margin)
                            pdf.multi_cell(0,10,txt=f"{q}: {a}")
                        pdf.ln(5)
                    elif isinstance(answer, list) and all(isinstance(elem, dict) for elem in answer):
                        pdf.set_x(pdf.l_margin)
                        pdf.multi_cell(0, 10, f"Question : {question}")
                        for result in answer:
                            line = ", ".join([f"{k}: {v}" for k, v in result.items()])
                            pdf.set_x(pdf.l_margin)
                            pdf.multi_cell(0,10,txt=f"{line}")
                        pdf.ln(5)
                    else:
                        pdf.set_x(pdf.l_margin)
                        pdf.multi_cell(0, 10, f"Question : {question}")
                        for value in answer:
                            pdf.set_x(pdf.l_margin)
                            pdf.multi_cell(0,10,txt=f"{value} , ")
                        pdf.ln(5)
                    
                    #graph insertion
                    slug=visualise.get_slug(question)
                    graph_path = Path(f"outputs/graphs/{run_id}_{slug}.png")
                    if graph_path.is_file():
                        if pdf.get_y() > 200:
                            pdf.add_page()
                        
                        pdf.image(str(graph_path), w=150)
                        pdf.ln(5)
                    
            elif key=="discarded_queries":
                if val:
                    pdf.set_font("Arial",size=18)
                    pdf.set_x(pdf.l_margin)
                    pdf.multi_cell(0,10,"Discarded AI Analysis",align='C')
                    pdf.set_font("Arial",10)
                    for question,answer in val.items():
                        pdf.set_x(pdf.l_margin)
                        pdf.multi_cell(0, 10, f"{question}")
                        query = answer["query"]
                        reason = answer["reason"]
                        pdf.set_x(pdf.l_margin)
                        pdf.multi_cell(0, 10, f"{query}:{reason}")
                pdf.ln(10)
                
        file_path = Path(f"outputs/reports/analysis_report_{run_id}.pdf")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        pdf.output(str(file_path))
        logger.info("Report generated successfully")
    except Exception as e:
        logger.error(f"Report generation failed : {e}")
            
