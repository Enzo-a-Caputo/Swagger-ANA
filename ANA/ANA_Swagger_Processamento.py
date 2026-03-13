import os
import json
import pandas as pd
from datetime import datetime, timedelta



class Processamento_JSON:
    def __init__(self):
        pass

    def P_HidroSerieChuva(self, pasta_json, pasta_saida_csv):
        """
        Lê todos os JSONs da pasta, transforma em DataFrames e salva CSVs,
        ignorando arquivos com estrutura inválida.
        
        Retorna:
            dict: Um dicionário {nome_arquivo_base: DataFrame} com todos os DFs.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)

        arquivos_json = [f for f in os.listdir(pasta_json) if f.endswith(".json")]
        
        todos_dataframes = {} # <-- MUDANÇA: 1. Inicializa o dicionário

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo}...")

            # Tenta carregar o JSON
            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Ignora se o conteúdo não for uma lista de dicionários
            if not isinstance(itens, list) or not all(isinstance(i, dict) for i in itens):
                print(f"⚠️ Estrutura inválida em {arquivo}, pulando arquivo.")
                continue

            registros = []

            for item in itens:
                try:
                    data_base = datetime.strptime(item["Data_Hora_Dado"][:10], "%Y-%m-%d")
                except (KeyError, ValueError):
                    continue  # pula se o campo de data for inválido

                nivel_consistencia = item.get("Nivel_Consistencia")

                for dia in range(1, 32):
                    campo = f"Chuva_{dia:02d}"
                    if campo in item:
                        chuva_str = item[campo]
                        try:
                            chuva = float(chuva_str) if chuva_str not in [None, ""] else None
                        except ValueError:
                            chuva = None

                        data_dia = data_base + timedelta(days=dia - 1)
                        registros.append({
                            "Data": data_dia,
                            "Precipitacao_mm": chuva,
                            "Nivel_Consistencia": nivel_consistencia
                        })

            # Cria DataFrame
            if registros:
                df = pd.DataFrame(registros)
                df = df.sort_values("Data").reset_index(drop=True)
            else:
                df = pd.DataFrame(columns=["Data", "Precipitacao_mm", "Nivel_Consistencia"])

            # Salva CSV
            nome_csv = arquivo.replace(".json", ".csv")
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d")
            print(f"✅ CSV salvo em {caminho_csv}")

            # <-- MUDANÇA: 2. Adiciona o DF ao dicionário
            nome_base = os.path.splitext(arquivo)[0]
            todos_dataframes[nome_base] = df

        return todos_dataframes # <-- MUDANÇA: 3. Retorna o dicionário completo

    def P_HidroSerieCota(self, pasta_json, pasta_saida_csv):
        """
        Lê todos os JSONs da pasta, transforma em DataFrames e salva CSVs,
        ignorando arquivos com estrutura inválida.
        
        Retorna:
            dict: Um dicionário {nome_arquivo_base: DataFrame} com todos os DFs.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)

        arquivos_json = [f for f in os.listdir(pasta_json) if f.endswith(".json")]
        
        todos_dataframes = {} # <-- MUDANÇA: 1. Inicializa o dicionário

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo}...")

            # Tenta carregar o JSON
            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Ignora se o conteúdo não for uma lista de dicionários
            if not isinstance(itens, list) or not all(isinstance(i, dict) for i in itens):
                print(f"⚠️ Estrutura inválida em {arquivo}, pulando arquivo.")
                continue

            registros = []

            for item in itens:
                try:
                    data_base = datetime.strptime(item["Data_Hora_Dado"][:10], "%Y-%m-%d")
                except (KeyError, ValueError):
                    continue  # pula se o campo de data for inválido

                nivel_consistencia = item.get("Nivel_Consistencia")

                for dia in range(1, 32):
                    campo = f"Cota_{dia:02d}"
                    if campo in item:
                        cota_str = item[campo]
                        try:
                            cota = float(cota_str) if cota_str not in [None, ""] else None
                        except ValueError:
                            cota = None

                        data_dia = data_base + timedelta(days=dia - 1)
                        registros.append({
                            "Data": data_dia,
                            "Cota_m": cota,
                            "Nivel_Consistencia": nivel_consistencia
                        })

            # Cria DataFrame
            if registros:
                df = pd.DataFrame(registros)
                df = df.sort_values("Data").reset_index(drop=True)
            else:
                df = pd.DataFrame(columns=["Data", "Cota_m", "Nivel_Consistencia"])

            # Salva CSV
            nome_csv = arquivo.replace(".json", ".csv")
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d")
            print(f"✅ CSV salvo em {caminho_csv}")

            # <-- MUDANÇA: 2. Adiciona o DF ao dicionário
            nome_base = os.path.splitext(arquivo)[0]
            todos_dataframes[nome_base] = df

        return todos_dataframes # <-- MUDANÇA: 3. Retorna o dicionário completo

    def P_HidroSerieVazao(self, pasta_json, pasta_saida_csv):
        """
        Lê todos os JSONs da pasta, transforma em DataFrames de VAZÃO e salva CSVs,
        ignorando arquivos com estrutura inválida.
        
        Retorna:
            dict: Um dicionário {nome_arquivo_base: DataFrame} com todos os DFs.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)

        arquivos_json = [f for f in os.listdir(pasta_json) if f.endswith(".json")]
        
        todos_dataframes = {} # <-- MUDANÇA: 1. Inicializa o dicionário

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo}...")

            # Tenta carregar o JSON
            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Ignora se o conteúdo não for uma lista de dicionários
            if not isinstance(itens, list) or not all(isinstance(i, dict) for i in itens):
                print(f"⚠️ Estrutura inválida em {arquivo}, pulando arquivo.")
                continue

            registros = []

            for item in itens:
                try:
                    data_base = datetime.strptime(item["Data_Hora_Dado"][:10], "%Y-%m-%d")
                except (KeyError, ValueError):
                    continue  # pula se o campo de data for inválido

                nivel_consistencia = item.get("Nivel_Consistencia")

                for dia in range(1, 32):
                    campo = f"Vazao_{dia:02d}"
                    if campo in item:
                        vazao_str = item[campo]
                        try:
                            vazao = float(vazao_str) if vazao_str not in [None, ""] else None
                        except ValueError:
                            vazao = None

                        data_dia = data_base + timedelta(days=dia - 1)
                        registros.append({
                            "Data": data_dia,
                            "Vazao_m3s": vazao,
                            "Nivel_Consistencia": nivel_consistencia
                        })

            # Cria DataFrame
            if registros:
                df = pd.DataFrame(registros)
                df = df.sort_values("Data").reset_index(drop=True)
            else:
                df = pd.DataFrame(columns=["Data", "Vazao_m3s", "Nivel_Consistencia"])

            # Salva CSV
            nome_csv = arquivo.replace(".json", ".csv")
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d")
            print(f"✅ CSV salvo em {caminho_csv}")

            # <-- MUDANÇA: 2. Adiciona o DF ao dicionário
            nome_base = os.path.splitext(arquivo)[0]
            todos_dataframes[nome_base] = df

        return todos_dataframes # <-- MUDANÇA: 3. Retorna o dicionário completo
    
    def P_HidroSerieCurvaDescarga(self, pasta_json, pasta_saida_csv):
        """
        Lê todos os JSONs de Curva de Descarga (Curva-Chave) da pasta.
        Cada item no JSON é um segmento da curva (Cota/Vazão).
        - Converte colunas de data e numéricas.
        - Garante que 'Periodo_Validade_Inicio' e 'Periodo_Validade_Fim' 
          sejam as primeiras colunas.
        - Salva CSVs e retorna um dicionário com todos os DataFrames.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)
        arquivos_json = [f for f in os.listdir(pasta_json) if f.lower().endswith(".json")]
        todos_dataframes = {}

        # Define colunas para conversão
        date_cols = ['Periodo_Validade_Inicio', 'Periodo_Validade_Fim', 'Data_Ultima_Alteracao']
        numeric_cols = [
            'Coef_a', 'Coef_h0', 'Coef_n', 'Coefa_0', 'Coefa_1', 'Coefa_2', 
            'Coefa_3', 'Cota_Maxima', 'Cota_Minima', 'Tabela_Passo_Cota'
        ]

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo} (Curva Descarga)...")

            # Tenta carregar o JSON
            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Validação básica
            if not isinstance(itens, list) or not itens:
                print(f"⚠️ Estrutura inválida ou vazia em {arquivo}, pulando.")
                continue

            # Converte a lista de registros diretamente para DataFrame
            try:
                df = pd.DataFrame(itens)
            except Exception as e:
                print(f"❌ Erro ao criar DataFrame de {arquivo}: {e}")
                continue

            # --- Processamento dos Dados ---

            # 1. Validar colunas de data principais
            if "Periodo_Validade_Inicio" not in df.columns or "Periodo_Validade_Fim" not in df.columns:
                print(f"⚠️ {arquivo} não contém colunas de período de validade, pulando.")
                continue

            # 2. Converter colunas de data
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # 3. Converter colunas numéricas
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 4. Reordenar colunas (Requisito principal do usuário)
            cols_restantes = [
                c for c in df.columns if c not in ['Periodo_Validade_Inicio', 'Periodo_Validade_Fim']
            ]
            # Constrói a nova ordem
            nova_ordem = ['Periodo_Validade_Inicio', 'Periodo_Validade_Fim'] + cols_restantes
            df = df[nova_ordem]

            # 5. Ordena pela data de início e cota mínima (para ordenar segmentos)
            df = df.sort_values(by=["Periodo_Validade_Inicio", "Cota_Minima"]).reset_index(drop=True)

            # --- Salvamento ---

            # Salva CSV
            nome_base = os.path.splitext(arquivo)[0]
            nome_csv = f"{nome_base}.csv"
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            
            # Salva com formato de hora
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d %H:%M:%S")
            print(f"✅ CSV de Curva Descarga salvo em {caminho_csv}")

            # Adiciona ao dicionário de retorno
            todos_dataframes[nome_base] = df

        return todos_dataframes
    
    def P_HidroSeriePerfilTransversal(self, pasta_json, pasta_saida_csv):
        """
        Lê todos os JSONs de Perfil Transversal da pasta.
        Cada item no JSON é um ponto (Cota/Distancia) de um levantamento.
        - Converte 'Data_Hora_Medicao' para datetime e a torna a primeira coluna.
        - Converte 'Cota' e 'Distancia' para numérico.
        - Salva CSVs e retorna um dicionário com todos os DataFrames.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)
        arquivos_json = [f for f in os.listdir(pasta_json) if f.lower().endswith(".json")]
        todos_dataframes = {}

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo} (Perfil Transversal)...")

            # Tenta carregar o JSON
            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Validação básica
            if not isinstance(itens, list) or not itens:
                print(f"⚠️ Estrutura inválida ou vazia em {arquivo}, pulando.")
                continue

            # Converte a lista de registros diretamente para DataFrame
            try:
                df = pd.DataFrame(itens)
            except Exception as e:
                print(f"❌ Erro ao criar DataFrame de {arquivo}: {e}")
                continue

            # --- Processamento dos Dados ---

            # 1. Valida e processa a Data
            if "Data_Hora_Medicao" not in df.columns:
                print(f"⚠️ {arquivo} não contém 'Data_Hora_Medicao', pulando.")
                continue
                
            df["Data"] = pd.to_datetime(df["Data_Hora_Medicao"], errors='coerce')
            df = df.dropna(subset=["Data"]) # Remove linhas com datas inválidas
            df = df.drop(columns=["Data_Hora_Medicao"]) # Remove a coluna original

            if df.empty:
                print(f"⚠️ Sem dados com datas válidas em {arquivo}, pulando.")
                continue
            
            # 2. Converte colunas de dados para numérico
            if 'Cota' in df.columns:
                df['Cota'] = pd.to_numeric(df['Cota'], errors='coerce')
            if 'Distancia' in df.columns:
                df['Distancia'] = pd.to_numeric(df['Distancia'], errors='coerce')

            # 3. Reordena colunas (coloca 'Data' em primeiro)
            cols = list(df.columns)
            cols.remove("Data")
            df = df[["Data"] + cols]

            # 4. Ordena pela data e pela distância
            df = df.sort_values(by=["Data", "Distancia"]).reset_index(drop=True)

            # --- Salvamento ---

            # Salva CSV
            nome_base = os.path.splitext(arquivo)[0]
            nome_csv = f"{nome_base}.csv"
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            
            # Salva com formato de hora
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d %H:%M:%S")
            print(f"✅ CSV de Perfil salvo em {caminho_csv}")

            # Adiciona ao dicionário de retorno
            todos_dataframes[nome_base] = df

        return todos_dataframes
    
    def P_HidroSerieQA(self, pasta_json, pasta_saida_csv):
        """
        Lê todos os JSONs de Qualidade de Água (QA) da pasta.
        Cada item no JSON é um registro (linha).
        - Converte 'Data_Hora_Dado' para datetime e a torna a primeira coluna.
        - Salva CSVs e retorna um dicionário com todos os DataFrames.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)
        arquivos_json = [f for f in os.listdir(pasta_json) if f.lower().endswith(".json")]
        todos_dataframes = {}

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo} (QA)...")

            # Tenta carregar o JSON
            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Validação básica
            if not isinstance(itens, list) or not itens:
                print(f"⚠️ Estrutura inválida ou vazia em {arquivo}, pulando.")
                continue

            # Converte a lista de registros diretamente para DataFrame
            try:
                df = pd.DataFrame(itens)
            except Exception as e:
                print(f"❌ Erro ao criar DataFrame de {arquivo}: {e}")
                continue

            # --- Processamento dos Dados ---

            # 1. Valida e processa a Data
            if "Data_Hora_Dado" not in df.columns:
                print(f"⚠️ {arquivo} não contém 'Data_Hora_Dado', pulando.")
                continue

            # Converte para datetime e remove linhas com datas inválidas
            df["Data"] = pd.to_datetime(df["Data_Hora_Dado"], errors='coerce')
            df = df.dropna(subset=["Data"])
            df = df.drop(columns=["Data_Hora_Dado"]) # Remove a coluna original

            if df.empty:
                print(f"⚠️ Sem dados com datas válidas em {arquivo}, pulando.")
                continue

            # 2. Corrige nome de coluna (se existir)
            if "Nilvel_ConsistÃªncia" in df.columns:
                df = df.rename(columns={"Nilvel_ConsistÃªncia": "Nivel_Consistencia"})
            
            # 3. Reordena colunas (coloca 'Data' em primeiro)
            cols = list(df.columns)
            cols.remove("Data")
            df = df[["Data"] + cols]

            # 4. Ordena pela data
            df = df.sort_values("Data").reset_index(drop=True)

            # --- Salvamento ---

            # Salva CSV
            nome_base = os.path.splitext(arquivo)[0]
            nome_csv = f"{nome_base}.csv"
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            
            # Salva com formato de hora, pois os dados originais possuem
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d %H:%M:%S")
            print(f"✅ CSV de QA salvo em {caminho_csv}")

            # Adiciona ao dicionário de retorno
            todos_dataframes[nome_base] = df

        return todos_dataframes
    
    def P_HidroSerieResumoDescarga(self, pasta_json, pasta_saida_csv):
        """
        Lê todos os JSONs de Resumo de Descarga da pasta.
        Cada item no JSON é um registro de medição (Cota, Vazão, Área, etc.).
        - Converte 'Data_Hora_Dado' para datetime e a torna a primeira coluna.
        - Converte colunas de medição para numérico.
        - Salva CSVs e retorna um dicionário com todos os DataFrames.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)
        arquivos_json = [f for f in os.listdir(pasta_json) if f.lower().endswith(".json")]
        todos_dataframes = {}

        # Colunas que esperamos que sejam numéricas, mas estão como string
        numeric_cols = [
            "Cota (cm)",
            "Area_Molhada (m2)",
            "Largura (m)",
            "Profundidade (m)",
            "Vazao (m3/s)",
            "Vel_Media (m/s)"
        ]

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo} (Resumo Descarga)...")

            # Tenta carregar o JSON
            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Validação básica
            if not isinstance(itens, list) or not itens:
                print(f"⚠️ Estrutura inválida ou vazia em {arquivo}, pulando.")
                continue

            # Converte a lista de registros diretamente para DataFrame
            try:
                df = pd.DataFrame(itens)
            except Exception as e:
                print(f"❌ Erro ao criar DataFrame de {arquivo}: {e}")
                continue

            # --- Processamento dos Dados ---

            # 1. Valida e processa a Data
            if "Data_Hora_Dado" not in df.columns:
                print(f"⚠️ {arquivo} não contém 'Data_Hora_Dado', pulando.")
                continue
                
            df["Data"] = pd.to_datetime(df["Data_Hora_Dado"], errors='coerce')
            df = df.dropna(subset=["Data"]) # Remove linhas com datas inválidas
            df = df.drop(columns=["Data_Hora_Dado"]) # Remove a coluna original

            if df.empty:
                print(f"⚠️ Sem dados com datas válidas em {arquivo}, pulando.")
                continue
            
            # 2. Converte colunas de dados para numérico
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 3. Reordena colunas (Requisito principal do usuário)
            cols_restantes = [c for c in df.columns if c != "Data"]
            nova_ordem = ["Data"] + cols_restantes
            df = df[nova_ordem]

            # 4. Ordena pela data
            df = df.sort_values(by="Data").reset_index(drop=True)

            # --- Salvamento ---

            # Salva CSV
            nome_base = os.path.splitext(arquivo)[0]
            nome_csv = f"{nome_base}.csv"
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            
            # Salva com formato de hora
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d %H:%M:%S")
            print(f"✅ CSV de Resumo Descarga salvo em {caminho_csv}")

            # Adiciona ao dicionário de retorno
            todos_dataframes[nome_base] = df

        return todos_dataframes
    
    def P_HidroSerieSedimentos(self, pasta_json, pasta_saida_csv):
        """
        Lê todos os JSONs de "Sedimentos" (Telemetria) da pasta.
        Cada item no JSON é um registro horário (Chuva, Cota, Vazão).
        - Converte 'Data_Hora_Medicao' para datetime e a torna a primeira coluna.
        - Converte colunas de medição para numérico.
        - Salva CSVs e retorna um dicionário com todos os DataFrames.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)
        arquivos_json = [f for f in os.listdir(pasta_json) if f.lower().endswith(".json")]
        todos_dataframes = {}

        # Colunas que esperamos que sejam numéricas
        numeric_cols = [
            "Chuva_Adotada",
            "Cota_Adotada",
            "Vazao_Adotada"
        ]
        
        # Colunas que esperamos que sejam datas
        date_cols = [
            "Data_Hora_Medicao",
            "Data_Atualizacao"
        ]

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo} (Telemetria/Sedimentos)...")

            # Tenta carregar o JSON
            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Validação básica
            if not isinstance(itens, list) or not itens:
                print(f"⚠️ Estrutura inválida ou vazia em {arquivo}, pulando.")
                continue

            # Converte a lista de registros diretamente para DataFrame
            try:
                df = pd.DataFrame(itens)
            except Exception as e:
                print(f"❌ Erro ao criar DataFrame de {arquivo}: {e}")
                continue

            # --- Processamento dos Dados ---

            # 1. Valida e processa a Data Principal
            if "Data_Hora_Medicao" not in df.columns:
                print(f"⚠️ {arquivo} não contém 'Data_Hora_Medicao', pulando.")
                continue

            # 2. Converte colunas de data
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # Remove linhas que falharam na conversão da data principal
            df = df.dropna(subset=["Data_Hora_Medicao"])
            if df.empty:
                print(f"⚠️ Sem dados com datas válidas em {arquivo}, pulando.")
                continue
            
            # 3. Converte colunas de dados para numérico
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 4. Reordena colunas (Requisito principal do usuário)
            cols_restantes = [c for c in df.columns if c != "Data_Hora_Medicao"]
            nova_ordem = ["Data_Hora_Medicao"] + cols_restantes
            df = df[nova_ordem]

            # 5. Ordena pela data
            df = df.sort_values(by="Data_Hora_Medicao").reset_index(drop=True)

            # --- Salvamento ---

            # Salva CSV
            nome_base = os.path.splitext(arquivo)[0]
            nome_csv = f"{nome_base}.csv"
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            
            # Salva com formato de hora
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d %H:%M:%S")
            print(f"✅ CSV de Telemetria salvo em {caminho_csv}")

            # Adiciona ao dicionário de retorno
            todos_dataframes[nome_base] = df

        return todos_dataframes


    def processar_jsons_gerar_csv(self, pasta_json, pasta_saida_csv):
        """
        Lê todos os JSONs da pasta, transforma em DataFrames e salva CSVs,
        ignorando arquivos com estrutura inválida.
        
        Retorna:
            dict: Um dicionário {nome_arquivo_base: DataFrame} com todos os DFs.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)

        arquivos_json = [f for f in os.listdir(pasta_json) if f.endswith(".json")]
        
        todos_dataframes = {} # <-- MUDANÇA: 1. Inicializa o dicionário

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo}...")

            # Tenta carregar o JSON
            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Ignora se o conteúdo não for uma lista de dicionários
            if not isinstance(itens, list) or not all(isinstance(i, dict) for i in itens):
                print(f"⚠️ Estrutura inválida em {arquivo}, pulando arquivo.")
                continue

            registros = []

            for item in itens:
                try:
                    data_base = datetime.strptime(item["Data_Hora_Dado"][:10], "%Y-%m-%d")
                except (KeyError, ValueError):
                    continue  # pula se o campo de data for inválido

                nivel_consistencia = item.get("Nivel_Consistencia")

                for dia in range(1, 32):
                    # ATENÇÃO: Esta função estava procurando "Cota_"
                    campo = f"Cota_{dia:02d}" 
                    if campo in item:
                        vazao_str = item[campo]
                        try:
                            vazao = float(vazao_str) if vazao_str not in [None, ""] else None
                        except ValueError:
                            vazao = None

                        data_dia = data_base + timedelta(days=dia - 1)
                        registros.append({
                            "Data": data_dia,
                            "Vazao": vazao, # E salvando como "Vazao"
                            "Nivel_Consistencia": nivel_consistencia
                        })

            # Cria DataFrame
            if registros:
                df = pd.DataFrame(registros)
                df = df.sort_values("Data").reset_index(drop=True)
            else:
                df = pd.DataFrame(columns=["Data", "Vazao", "Nivel_Consistencia"])

            # Salva CSV
            nome_csv = arquivo.replace(".json", ".csv")
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d")
            print(f"✅ CSV salvo em {caminho_csv}")
            
            # <-- MUDANÇA: 2. Adiciona o DF ao dicionário
            nome_base = os.path.splitext(arquivo)[0]
            todos_dataframes[nome_base] = df

        return todos_dataframes # <-- MUDANÇA: 3. Retorna o dicionário completo

    def P_HidroinfoanaSerieTelemetricaDetalhada(self, pasta_json, pasta_saida_csv, tipo_dado="precipitacao"):
        """
        Lê JSONs de telemetria horária e salva CSVs separados.
        Retorna um dicionário {nome_arquivo: DataFrame}.

        Parâmetros:
            pasta_json (str): pasta com os arquivos JSON.
            pasta_saida_csv (str): pasta para salvar os CSVs.
            tipo_dado (str): tipo de dado a processar: "precipitacao", "vazao" ou "cota".

        - Ignora arquivos vazios ou inválidos.
        - Garante que DataFrame só será criado se houver registros válidos.
        """
        os.makedirs(pasta_saida_csv, exist_ok=True)

        # Mapeamento do tipo de dado para o campo correspondente no JSON
        campo_dado = {
            "precipitacao": "Chuva_Adotada",
            "vazao": "Vazao_Adotada",
            "cota": "Cota_Adotada"
        }.get(tipo_dado.lower())

        if campo_dado is None:
            raise ValueError("tipo_dado deve ser 'precipitacao', 'vazao' ou 'cota'")

        def _parse_datetime(valor):
            if valor is None:
                return None
            s = str(valor).strip()
            if s.endswith(".0"):
                s = s[:-2]
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
            try:
                return pd.to_datetime(s, errors="coerce").to_pydatetime()
            except Exception:
                return None

        arquivos_json = [f for f in os.listdir(pasta_json) if f.lower().endswith(".json")]
        resultados = {}

        for arquivo in arquivos_json:
            caminho_json = os.path.join(pasta_json, arquivo)
            print(f"\n📂 Processando {arquivo}...")

            try:
                with open(caminho_json, "r", encoding="utf-8") as f:
                    itens = json.load(f)
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            if not itens:
                print(f"⚠️ Arquivo {arquivo} está vazio, pulando.")
                continue

            if not isinstance(itens, list) or not all(isinstance(i, dict) for i in itens):
                print(f"⚠️ Estrutura inválida em {arquivo}, pulando arquivo.")
                continue

            registros = []
            for item in itens:
                dt = _parse_datetime(item.get("Data_Hora_Medicao"))
                if dt is None:
                    continue

                valor_raw = item.get(campo_dado)
                if valor_raw in [None, ""]:
                    valor = None
                else:
                    try:
                        valor = float(str(valor_raw).replace(",", "."))
                    except ValueError:
                        valor = None

                status_raw = item.get(f"{campo_dado}_Status")
                nivel_consistencia = None if status_raw in [None, ""] else str(status_raw)

                registros.append({
                    "Data": dt,
                    tipo_dado.capitalize(): valor,
                    "Nivel_Consistencia": nivel_consistencia
                })

            if not registros:
                print(f"⚠️ Nenhum registro válido em {arquivo}, pulando arquivo.")
                continue

            df = pd.DataFrame(registros).sort_values("Data").reset_index(drop=True)
            nome_csv = os.path.splitext(arquivo)[0] + f"_{tipo_dado}.csv"
            caminho_csv = os.path.join(pasta_saida_csv, nome_csv)
            df.to_csv(caminho_csv, sep=";", index=False, date_format="%Y-%m-%d %H:%M:%S")
            print(f"✅ CSV salvo em {caminho_csv}")

            resultados[os.path.splitext(arquivo)[0]] = df

        return resultados


    


    def Agregar_Diario(self, pasta_csv_entrada, pasta_csv_saida):
        """
        Lê todos os CSVs (sep=';') da pasta de entrada que tenham 3 colunas:
        [Data, Valor, Nivel_Consistencia] e gera séries diárias:
        - Valor: soma das medições do mesmo dia
        - Nivel_Consistencia: mantém o primeiro valor encontrado do dia

        Salva na pasta de saída com sufixo '_diario.csv'.
        Retorna um dicionário {nome_arquivo: DataFrame_diario}.
        Arquivos com estrutura inesperada são ignorados.
        """
        os.makedirs(pasta_csv_saida, exist_ok=True)

        arquivos = [f for f in os.listdir(pasta_csv_entrada) if f.lower().endswith(".csv")]
        resultados = {}

        for arquivo in arquivos:
            caminho = os.path.join(pasta_csv_entrada, arquivo)
            print(f"\n📂 Processando {arquivo}...")

            # Lê CSV
            try:
                df = pd.read_csv(caminho, sep=";")
            except Exception as e:
                print(f"❌ Erro ao ler {arquivo}: {e}")
                continue

            # Valida se há pelo menos 3 colunas
            if df.shape[1] < 3:
                print(f"⚠️ Estrutura inesperada em {arquivo}. Pulando.")
                continue

            # Extrai colunas por índice
            col_data = df.columns[0]
            col_valor = df.columns[1]
            col_consistencia = df.columns[2]

            # Converte Data e Valor
            df[col_data] = pd.to_datetime(df[col_data], errors="coerce")
            df[col_valor] = pd.to_numeric(df[col_valor], errors="coerce")

            # Remove linhas sem Data
            df = df.dropna(subset=[col_data])
            if df.empty:
                print(f"⚠️ Sem linhas válidas em {arquivo}. Pulando.")
                continue

            # Extrai somente a data (sem hora)
            df["Data_Dia"] = df[col_data].dt.date

            # Agregação diária
            df_diario = (
                df.groupby("Data_Dia", as_index=False)
                .agg({
                    col_valor: "sum",
                    col_consistencia: "first"
                })
                .rename(columns={col_valor: col_valor, col_consistencia: col_consistencia, "Data_Dia": col_data})
                .sort_values(col_data)
                .reset_index(drop=True)
            )

            # Salva com sufixo _diario.csv
            nome_out = os.path.splitext(arquivo)[0] + "_diario.csv"
            caminho_out = os.path.join(pasta_csv_saida, nome_out)

            df_diario.to_csv(caminho_out, sep=";", index=False, date_format="%Y-%m-%d")
            print(f"✅ CSV diário salvo em {caminho_out}")

            resultados[os.path.splitext(arquivo)[0]] = df_diario

        return resultados

            