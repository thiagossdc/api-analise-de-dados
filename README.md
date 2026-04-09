# API de Análise de Dados

API desenvolvida para análise estatística e visualização de dados, seguindo os princípios de Clean Code e arquitetura hexagonal.

## Funcionalidades

- Upload de dados em formatos CSV e JSON
- Análises estatísticas (média, mediana, desvio padrão, etc.)
- Geração de gráficos (barras, linhas, pizza)
- Relatórios em PDF, CSV e JSON
- Integração com ferramentas de BI (Tableau, Power BI)

## Tecnologias

- **Linguagem**: Python 3.9+
- **Framework**: FastAPI
- **Banco de Dados**: SQLite
- **Bibliotecas**: pandas, matplotlib, plotly, pydantic

## Instalação

1. Clone o repositório
2. Crie um ambiente virtual: `python -m venv venv`
3. Ative o ambiente virtual: `source venv/bin/activate`
4. Instale as dependências: `pip install -r requirements.txt`
5. Execute a aplicação: `uvicorn main:app --reload`

## Serviço de Mensageria

O sistema implementa um serviço de mensageria assíncrona com os seguintes tópicos:
- `file.uploaded`: Arquivo enviado com sucesso
- `analysis.completed`: Análise estatística concluída
- `chart.generated`: Gráfico gerado com sucesso
- `report.generated`: Relatório gerado com sucesso
- `notification`: Notificações gerais

Os eventos são processados em background thread e logados automaticamente.

## Uso

A documentação da API está disponível em `/docs`