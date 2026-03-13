# -*- coding: utf-8 -*-
"""
Projeto: Swagger-ANA
Autor: Enzo Augusto Caputo - Engenheiro Civil (UFMG)
Contato: zoencaputo@gmail.com

Descrição:
Este módulo faz parte de uma ferramenta desenvolvida para facilitar o acesso,
download e processamento espacial de dados do Webservice da Agência Nacional 
de Águas e Saneamento Básico (ANA).

Data de Criação: 2026
"""



import os
import json
import time
import requests
from datetime import datetime, timedelta
from .ANA_Swagger_Base_GET import *
from .ANA_Swagger_Autenticacao import gerar_token_ana
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point




class Download_JSON:
    def __init__(self):
        self.Base = Base_API()


    def _verificar_e_renovar_token(self, token, validade_token, identificador, senha, margem_minutos=2):
        """Renova o token se ele estiver vencido ou prestes a vencer."""
        agora = datetime.now(validade_token.tzinfo)
        tempo_restante = (validade_token - agora).total_seconds() / 60

        if tempo_restante <= margem_minutos:
            token_novo, validade_nova = gerar_token_ana(identificador, senha)
            if token_novo != token:
                return token_novo, validade_nova
            else:
                print("Token renovado, mas continua o mesmo. Aguardando e tentando novamente...")
                time.sleep(60)
                return self._verificar_e_renovar_token(token_novo, validade_nova, identificador, senha)

        return token, validade_token
    


    def D_HidroSerieChuva(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025):
        """Baixa dados de chuva por ano para uma estação e salva em JSON."""
        os.makedirs(pasta_saida, exist_ok=True)
        token, validade_token = gerar_token_ana(identificador, senha)
        dados_todos_anos = []

        for ano in range(ano_inicial, ano_final):
            data_inicial = f"{ano}-01-01"
            data_final = f"{ano+1}-01-01"

            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            print(f"Solicitando dados de {data_inicial} a {data_final}...")

            try:
                resultado = self.Base.get_HidroSerieChuva(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    resultado = self.Base.get_HidroSerieChuva(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
                else:
                    raise

            itens = resultado.get("items", [])
            print(f"{len(itens)} itens retornados.")
            dados_todos_anos.extend(itens)

        caminho_json = os.path.join(pasta_saida, f"chuva_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_todos_anos, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em {caminho_json}")


    def D_HidroSerieCota(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025):
        """Baixa dados de cota por ano para uma estação e salva em JSON."""
        os.makedirs(pasta_saida, exist_ok=True)
        token, validade_token = gerar_token_ana(identificador, senha)
        dados_todos_anos = []

        for ano in range(ano_inicial, ano_final):
            data_inicial = f"{ano}-01-01"
            data_final = f"{ano+1}-01-01"

            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            print(f"Solicitando dados de {data_inicial} a {data_final}...")

            try:
                resultado = self.Base.get_HidroSerieCota(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    resultado = self.Base.get_HidroSerieCota(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
                else:
                    raise

            itens = resultado.get("items", [])
            print(f"{len(itens)} itens retornados.")
            dados_todos_anos.extend(itens)

        caminho_json = os.path.join(pasta_saida, f"Cota_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_todos_anos, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em {caminho_json}")


    def D_HidroSerieVazao(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025):
        """Baixa dados de vazão por ano para uma estação e salva em JSON."""
        os.makedirs(pasta_saida, exist_ok=True)
        token, validade_token = gerar_token_ana(identificador, senha)
        dados_todos_anos = []

        for ano in range(ano_inicial, ano_final):
            data_inicial = f"{ano}-01-01"
            data_final = f"{ano+1}-01-01"

            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            print(f"Solicitando dados de {data_inicial} a {data_final}...")

            try:
                resultado = self.Base.get_HidroSerieVazao(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    resultado = self.Base.get_HidroSerieVazao(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
                else:
                    raise

            itens = resultado.get("items", [])
            print(f"{len(itens)} itens retornados.")
            dados_todos_anos.extend(itens)

        caminho_json = os.path.join(pasta_saida, f"Vazao_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_todos_anos, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em {caminho_json}")


    def D_HidroSerieCurvaDescarga(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025):
        """Baixa dados de Curva de Descarga por ano para uma estação e salva em JSON."""
        os.makedirs(pasta_saida, exist_ok=True)
        token, validade_token = gerar_token_ana(identificador, senha)
        dados_todos_anos = []

        for ano in range(ano_inicial, ano_final):
            data_inicial = f"{ano}-01-01"
            data_final = f"{ano+1}-01-01"

            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            print(f"Solicitando dados de {data_inicial} a {data_final}...")

            try:
                resultado = self.Base.get_HidroSerieCurvaDescarga(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    resultado = self.Base.get_HidroSerieCurvaDescarga(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
                else:
                    raise

            itens = resultado.get("items", [])
            print(f"{len(itens)} itens retornados.")
            dados_todos_anos.extend(itens)

        caminho_json = os.path.join(pasta_saida, f"CurvaDescarga_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_todos_anos, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em {caminho_json}")


    def D_HidroSeriePerfilTransversal(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025):
        """Baixa dados de Perfil Transversal por ano para uma estação e salva em JSON."""
        os.makedirs(pasta_saida, exist_ok=True)
        token, validade_token = gerar_token_ana(identificador, senha)
        dados_todos_anos = []

        for ano in range(ano_inicial, ano_final):
            data_inicial = f"{ano}-01-01"
            data_final = f"{ano+1}-01-01"

            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            print(f"Solicitando dados de {data_inicial} a {data_final}...")

            try:
                resultado = self.Base.get_HidroSeriePerfilTransversal(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    resultado = self.Base.get_HidroSeriePerfilTransversal(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
                else:
                    raise

            itens = resultado.get("items", [])
            print(f"{len(itens)} itens retornados.")
            dados_todos_anos.extend(itens)

        caminho_json = os.path.join(pasta_saida, f"PerfilTransversal_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_todos_anos, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em {caminho_json}")


    def D_HidroSerieQA(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025):
        """Baixa dados de Qualidade de Água (QA) por ano para uma estação e salva em JSON."""
        os.makedirs(pasta_saida, exist_ok=True)
        token, validade_token = gerar_token_ana(identificador, senha)
        dados_todos_anos = []

        for ano in range(ano_inicial, ano_final):
            data_inicial = f"{ano}-01-01"
            data_final = f"{ano+1}-01-01"

            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            print(f"Solicitando dados de {data_inicial} a {data_final}...")

            try:
                resultado = self.Base.get_HidroSerieQA(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    resultado = self.Base.get_HidroSerieQA(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
                else:
                    raise

            itens = resultado.get("items", [])
            print(f"{len(itens)} itens retornados.")
            dados_todos_anos.extend(itens)

        caminho_json = os.path.join(pasta_saida, f"QA_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_todos_anos, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em {caminho_json}")


    def D_HidroSerieResumoDescarga(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025):
        """Baixa dados de Resumo de Descarga por ano para uma estação e salva em JSON."""
        os.makedirs(pasta_saida, exist_ok=True)
        token, validade_token = gerar_token_ana(identificador, senha)
        dados_todos_anos = []

        for ano in range(ano_inicial, ano_final):
            data_inicial = f"{ano}-01-01"
            data_final = f"{ano+1}-01-01"

            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            print(f"Solicitando dados de {data_inicial} a {data_final}...")

            try:
                resultado = self.Base.get_HidroSerieResumoDescarga(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    resultado = self.Base.get_HidroSerieResumoDescarga(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
                else:
                    raise

            itens = resultado.get("items", [])
            print(f"{len(itens)} itens retornados.")
            dados_todos_anos.extend(itens)

        caminho_json = os.path.join(pasta_saida, f"ResumoDescarga_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_todos_anos, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em {caminho_json}")


    def D_HidroSerieSedimentos(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025):
        """Baixa dados de Sedimentos por ano para uma estação e salva em JSON."""
        os.makedirs(pasta_saida, exist_ok=True)
        token, validade_token = gerar_token_ana(identificador, senha)
        dados_todos_anos = []

        for ano in range(ano_inicial, ano_final):
            data_inicial = f"{ano}-01-01"
            data_final = f"{ano+1}-01-01"

            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            print(f"Solicitando dados de {data_inicial} a {data_final}...")

            try:
                resultado = self.Base.get_HidroSerieSedimentos(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    resultado = self.Base.get_HidroSerieSedimentos(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
                else:
                    raise

            itens = resultado.get("items", [])
            print(f"{len(itens)} itens retornados.")
            dados_todos_anos.extend(itens)

        caminho_json = os.path.join(pasta_saida, f"Sedimentos_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_todos_anos, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em {caminho_json}")


    def D_HidroSerieDados(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025):
        """Baixa dados gerais (HidroSerieDados) por ano para uma estação e salva em JSON."""
        os.makedirs(pasta_saida, exist_ok=True)
        token, validade_token = gerar_token_ana(identificador, senha)
        dados_todos_anos = []

        for ano in range(ano_inicial, ano_final):
            data_inicial = f"{ano}-01-01"
            data_final = f"{ano+1}-01-01"

            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            print(f"Solicitando dados de {data_inicial} a {data_final}...")

            try:
                resultado = self.Base.get_HidroSerieDados(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    resultado = self.Base.get_HidroSerieDados(token, codigo_estacao, tipo_filtro_data, data_inicial, data_final)
                else:
                    raise

            itens = resultado.get("items", [])
            print(f"{len(itens)} itens retornados.")
            dados_todos_anos.extend(itens)

        caminho_json = os.path.join(pasta_saida, f"Dados_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_todos_anos, f, ensure_ascii=False, indent=2)
        print(f"JSON salvo em {caminho_json}")


    def D_HidroinfoanaSerieTelemetricaDetalhada(self, identificador, senha, codigo_estacao, pasta_saida, tipo_filtro_data='DATA_LEITURA', ano_inicial=1900, ano_final=2025, intervalo_dias="DIAS_30"):
        """
        Baixa dados telemétricos de uma estação entre dois anos,
        percorrendo intervalos de 30 dias (por padrão) para garantir cobertura.
        """
        os.makedirs(pasta_saida, exist_ok=True)

        dias_intervalo = 30
        token, validade_token = gerar_token_ana(identificador, senha)

        data_inicio = datetime(ano_inicial, 1, 1) - timedelta(days=30)
        data_fim = datetime(ano_final, 12, 31) + timedelta(days=30)

        dados_coletados = []
        data_atual = data_fim

        print(f"Iniciando download para estação {codigo_estacao} ({ano_inicial}-{ano_final})")

        while data_atual >= data_inicio:
            token, validade_token = self._verificar_e_renovar_token(token, validade_token, identificador, senha)

            data_intervalo_inicio = max(data_atual - timedelta(days=dias_intervalo - 1), data_inicio)
            data_str = data_atual.strftime('%Y-%m-%d')

            try:
                resultado = self.Base.get_HidroinfoanaSerieTelemetricaDetalhada(
                    token, codigo_estacao, tipo_filtro_data,
                    data_str, intervalo_dias
                )
                dados_coletados.extend(resultado.get("items", []))
                progresso = ((data_fim - data_atual).days / (data_fim - data_inicio).days * 100)
                print(f"Progresso: {progresso:.1f}% - Coletado até {data_intervalo_inicio.date()}")

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    print("Erro 401: Token expirado. Tentando renovar...")
                    token, validade_token = gerar_token_ana(identificador, senha)
                    continue
                else:
                    print(f"Erro: {str(e)} - nova tentativa em 5 minutos")
                    time.sleep(300)
                    continue

            data_atual = data_intervalo_inicio - timedelta(days=1)
            time.sleep(0)

        # Filtragem final por data exata
        data_inicio_real = datetime(ano_inicial, 1, 1)
        data_fim_real = datetime(ano_final, 12, 31, 23, 59, 59)

        dados_filtrados = [
            item for item in dados_coletados
            if data_inicio_real <= datetime.strptime(item['Data_Hora_Medicao'], '%Y-%m-%d %H:%M:%S.%f') <= data_fim_real
        ]

        dados_filtrados.sort(key=lambda x: datetime.strptime(x['Data_Hora_Medicao'], '%Y-%m-%d %H:%M:%S.%f'))

        caminho_json = os.path.join(pasta_saida, f"telemetria_estacao_{codigo_estacao}.json")
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(dados_filtrados, f, ensure_ascii=False, indent=2)

        print(f"Download concluído. Registros totais: {len(dados_coletados)} | Dentro do período: {len(dados_filtrados)}")
        print(f"JSON salvo em {caminho_json}")


