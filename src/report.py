from fpdf import FPDF
import json
from pathlib import Path
import visualise
import logging

logger = logging.getLogger(__name__)

def add_section(pdf, text):
    """Insert a stylized section into the PDF report.

    Args:
        pdf (FPDF): The active FPDF document instance.
        text (str): The heading text to be displayed for the new section.
    """
    pdf.set_font("Arial", "BU", 18)
    pdf.set_text_color(255, 0, 0)
    pdf.ln(5)
    pdf.set_x(pdf.l_margin)
    pdf.multi_cell(0, 10, text, align='C')
    pdf.set_text_color(0, 0, 0)
    pdf.ln(3)


def add_question(pdf, text):
    """Insert a formatted question into the PDF report

    Args:
        pdf (FPDF): The active FPDF document instance.
        text (str): The natural language analytical question to be inserted.
    """
    pdf.set_font("Arial", "B", 14)
    pdf.set_x(pdf.l_margin)
    pdf.multi_cell(0, 8, f"Question: {text}")
    pdf.ln(1)


def add_answer(pdf, text):
    """Insert a formatted analysis answer into the PDF report

    Args:
        pdf (FPDF): The active FPDF document instance.
        text (str): The natural language detailed answer to be inserted.
    """
    pdf.set_x(pdf.l_margin)
    pdf.set_font("Arial", "", 12)
    pdf.multi_cell(0, 8, text)


def add_table(pdf, data):
    """Insert a dynamic structured table into the PDF report
    
    Automatically calculates column widths based on the number of keys 
    in the first record. This function applies specialized formatting:
        - Headers are rendered in bold blue.
        - Floating-point values are rounded to two decimal places.
        - Cell text longer than 25 characters is truncated with an ellipsis.

    Args:
        pdf (FPDF): The active FPDF document instance.
        data (list[dict]): A list of dictionaries where each dictionary represents a row.
    """
    if not data:
        return

    headers = list(data[0].keys())
    col_width = (pdf.w - 2 * pdf.l_margin) / len(headers)
    
    pdf.set_font("Arial", "B", 12)
    pdf.set_text_color(0, 0, 255)
    for header in headers:
        pdf.cell(col_width, 8, str(header), border=1)
    pdf.set_text_color(0, 0, 0)
    pdf.ln()

    pdf.set_font("Arial", "", 12)
    for row in data:
        for header in headers:
            value=row.get(header,"")
            if(isinstance(value,float)):
                value=round(value,2)
                
            text=str(value)
            if len(text)>25:
                text=text[:20]+"..."
            pdf.cell(col_width, 8, str(text), border=1)
        pdf.ln()

    pdf.ln(3)


def add_graph(pdf, graph_path):
    """Insert the graph from local path to PDF report without cutting it between pages

    Args:
        pdf (FPDF): The active FPDF document instance.
        graph_path (Path): The local path where graph is stored
    """
    if graph_path.is_file():
        # Prevent cutting graph across pages
        if pdf.get_y() + 80 > pdf.page_break_trigger:
            pdf.add_page()

        pdf.image(str(graph_path),w=170)
        pdf.ln(5)


def read_json(run_id):
    """Load and deserialize the JSON results for a specific pipeline execution.
    
    Args:
        run_id (int): The unique identifier for the current pipeline execution, used to locate specific JSON file

    Returns:
        dict: The entire JSON file is transformed into a dictionary
    """
    with open(f"outputs/json/results_{run_id}.json", 'r', encoding='utf-8') as file:
        return json.load(file)


def create_report(run_id):
    """Generate a comprehensive PDF analysis report for a specific pipeline run.
    
    Consolidates data from the local JSON storage and filesystem-safe graphs to build a multi-section document including:
        - META-DATA (timestamps, source names, etc.)
        - Dataset statistics (row counts, cleaning metrics)
        - AI analysis results and visualizations
        - The queries discarded by pipeline - with the reason

    Args:
        run_id (int): The unique identifier of current pipeline run, used to retrieve data and save the final report.
        
    """
    pdf = FPDF()
    pdf.set_top_margin(20)
    pdf.set_auto_page_break(auto=True, margin=15)

    try:
        output_data = read_json(run_id)
        pdf.add_page()
        #adding title
        pdf.set_font("Arial", "BU", 22)
        pdf.set_text_color(101, 67, 33)
        pdf.ln(5)
        pdf.set_x(pdf.l_margin)
        pdf.multi_cell(0, 10, "Analysis Report", align='C')
        pdf.set_text_color(0, 0, 0)
        pdf.ln(3)
        

        for key, val in output_data.items():
            if key == "file_data":
                add_section(pdf, "Meta-Data of File")
                
                pdf.set_font("Arial", "", 12)
                for metric, answer in val.items():
                    pdf.set_x(pdf.l_margin)
                    pdf.multi_cell(0, 8, f"{metric} : {answer}")
                pdf.ln(5)

            elif key == "file_info":
                add_section(pdf, "Information of File")

                pdf.set_font("Arial", "", 12)
                for metric, answer in val.items():
                    pdf.set_x(pdf.l_margin)
                    pdf.multi_cell(0, 8, f"{metric} : {answer}")
                pdf.ln(5)

            elif key == "analysis_result":
                add_section(pdf, "Result after AI Analysis")

                for question, answer in val.items():

                    add_question(pdf, question)
                    if isinstance(answer, (int, float, str)):
                        add_answer(pdf, f"Answer: {answer}")

                    elif isinstance(answer, dict):
                        for k, v in answer.items():
                            add_answer(pdf, f"{k}: {v}")

                    elif isinstance(answer, list) and answer and all(isinstance(elem, dict) for elem in answer):
                        add_table(pdf, answer)

                    else:
                        for value in answer:
                            add_answer(pdf, str(value))

                    # Graph insertion
                    slug = visualise.get_slug(question)
                    graph_path = Path(f"outputs/graphs/{run_id}_{slug}.png")
                    add_graph(pdf, graph_path)

                    pdf.ln(5)  

            elif key == "discarded_queries":
                if val:
                    add_section(pdf, "Discarded AI Analysis")

                    for question, answer in val.items():
                        add_question(pdf, question)

                        query = answer.get("query", "")
                        reason = answer.get("reason", "")

                        add_answer(pdf, f"Query: {query}")
                        add_answer(pdf, f"Reason: {reason}")

                    pdf.ln(5)

        file_path = Path(f"outputs/reports/analysis_report_{run_id}.pdf")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        pdf.output(str(file_path))
        logger.info("Report generated successfully")

    except Exception as e:
        logger.error(f"Report generation failed : {e}")