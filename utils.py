import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import io
import base64
from typing import Dict, Any

def process_csv(file: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(io.BytesIO(file))
    except Exception as e:
        raise ValueError(f"Erro ao processar CSV: {str(e)}")

def process_json(file: bytes) -> pd.DataFrame:
    try:
        return pd.read_json(io.BytesIO(file))
    except Exception as e:
        raise ValueError(f"Erro ao processar JSON: {str(e)}")

def generate_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    return {
        "count": len(df),
        "columns": list(df.columns),
        "numeric_columns": df.select_dtypes(include=["number"]).columns.tolist(),
        "summary": df.describe().to_dict(),
        "missing_values": df.isnull().sum().to_dict()
    }

def generate_bar_chart(df: pd.DataFrame, column: str) -> str:
    fig = px.bar(df, x=column, y=df.columns[1])
    return fig.to_html(full_html=False)

def generate_line_chart(df: pd.DataFrame, column: str) -> str:
    fig = px.line(df, x=column, y=df.columns[1])
    return fig.to_html(full_html=False)

def generate_pie_chart(df: pd.DataFrame, column: str) -> str:
    fig = px.pie(df, names=column)
    return fig.to_html(full_html=False)

def generate_report(df: pd.DataFrame, format: str = "pdf") -> str:
    if format == "pdf":
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas

        buffer = io.BytesIO()
        p = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter

        p.drawString(100, height - 100, "Relat&oacute;rio de An&aacute;lise de Dados")
        p.drawString(100, height - 120, f"N&uacute;mero de registros: {len(df)}")

        p.showPage()
        p.save()
        buffer.seek(0)
        return base64.b64encode(buffer.read()).decode("utf-8")
    elif format == "csv":
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        return base64.b64encode(buffer.read()).decode("utf-8")
    elif format == "json":
        buffer = io.BytesIO()
        df.to_json(buffer)
        buffer.seek(0)
        return base64.b64encode(buffer.read()).decode("utf-8")
    else:
        raise ValueError("Formato n&atilde;o suportado")
