from fpdf import FPDF
import json
import logging

logger=logging.getLogger(__name__)

title="Analysis Report"

class PDF(FPDF):
    def header(self):
        #font
        self.set_font('Arial','B',22)
        
        #Calculate width of title
        title_w=self.get_string_width(title)+6
        doc_w=self.w
        self.set_x((doc_w-title_w)/2)
        
        #colors of frame,background,and text
        self.set_draw_color(255, 251, 0) #yellow
        self.set_fill_color(5, 5, 5) #black
        self.set_text_color(252, 250, 250) #white
        self.set_line_width(1)
        
        #Title
        self.cell(title_w,10,title,border=1,ln=1,align='C',fill=1)
        self.ln(10)
        
    def footer(self):
        # Set position of the footer
        self.set_y(-15)
        
        # set font
        self.set_font('helvetica', 'I', 8)
        
        self.set_text_color(169,169,169) #grey
        
        # Page number
        self.cell(0, 10, f'Page {self.page_no()}', align='C')
        

def read_json(run_id):
    with open(f"outputs/results_{run_id}.json",'r',encoding='utf-8') as file:
        output_data=json.load(file)
        return output_data
    
def create_report(run_id):
    pdf=PDF()
    pdf.add_page()
    try:
        output_data=read_json(run_id)
        
        for key,val in output_data.items():
            if key=="file_data":
                pdf.set_font("Arial",size=18)
                pdf.multi_cell(0,10,"Meta-Data of File",ln=True,align='C')
                pdf.set_font("Arial",size=16)
                for metric,answer in val.items():
                    pdf.multi_cell(0,10,txt=f"{metric} : {answer}",ln=1)
                pdf.ln(10)
                    
            elif key=="file_info":
                pdf.set_font("Arial",size=18)
                pdf.multi_cell(0,10,"Information of File",ln=True,align='C')
                pdf.set_font("Arial",size=14)
                for metric,answer in val.items():
                    pdf.multi_cell(0,10,txt=f"{metric} : {answer}",ln=1)
                pdf.ln(10)
                    
            elif key=="analysis_result":
                pdf.set_font("Arial",size=18)
                pdf.multi_cell(0,10,"Result after AI Analysis",ln=True,align='C')
                pdf.set_font("Arial",size=12)
                for question,answer in val.items():
                    pdf.multi_cell(0,10,txt=f"{question} : {answer}",ln=1)
                pdf.ln(10)
                    
            elif key=="discarded_queries":
                pdf.set_font("Arial",size=18)
                pdf.multi_cell(0,10,"Discarded AI Analysis",ln=True,align='C')
                pdf.set_font("Arial",10)
                for question,answer in val.items():
                    pdf.multi_cell(0,10,f"{question}",ln=1)
                    query=answer["query"]
                    reason=answer["reason"]
                    pdf.multi_cell(120,10,f"{query}:{reason}",ln=1)
                pdf.ln(10)
                
        pdf.output(f"outputs/reports/analysis_report_{run_id}.pdf")
        logger.info("Report generated successfully")
    except Exception as e:
        logger.error(f"Report generation failed : {e}")
            
