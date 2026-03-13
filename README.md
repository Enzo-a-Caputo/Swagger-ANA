# Swagger-ANA: Interface Hidro Webservice 🌊

Este repositório fornece uma biblioteca em Python para automação do download e processamento de dados hidrometeorológicos através do **Hidro Webservice**, a nova API baseada em Swagger da Agência Nacional de Águas e Saneamento Básico (ANA).

O projeto foi estruturado para facilitar o trabalho de engenheiros e pesquisadores, transformando requisições complexas em dados prontos para análise em `Pandas DataFrame`.

## 📌 Funcionalidades Principais
* 🛰️ **Consulta de Inventário**: Localização e metadados de estações fluviométricas e pluviométricas.
* 🌧️ **Séries Históricas**: Download automatizado de dados de Precipitação (Chuvas).
* 💧 **Dados Fluviométricos**: Acesso a séries de Vazões e Cotas.
* ⚙️ **Processamento Integrado**: Conversão automática de retornos da API para formatos estruturados.

## 🏗️ Estrutura do Repositório
O código é modular, separando a lógica de comunicação da lógica de aplicação:
* **`ANA_Swagger_Base_GET.py`**: Gerencia as requisições HTTP e a comunicação base com o Swagger.
* **`ANA_Swagger_Autenticacao.py`**: Trata os protocolos de acesso necessários.
* **`ANA_Swagger_Aplicacoes.py`**: Contém as funções de alto nível para busca de dados específicos.
* **`ANA_Swagger_Processamento.py`**: Realiza o tratamento, limpeza e estruturação dos dados brutos.
* **`ANA_Swagger_Download.py`**: Facilita o salvamento dos dados em formato local.

## 🚀 Como Começar
### Pré-requisitos
* Python 3.10 ou superior.
* Bibliotecas: `requests`, `pandas`.

### Instalação rápida
```bash
git clone [https://github.com/Enzunbi/Swagger-ANA.git](https://github.com/Enzunbi/Swagger-ANA.git)
cd Swagger-ANA
pip install -r requirements.txt
