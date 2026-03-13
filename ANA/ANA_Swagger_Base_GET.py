import requests
from datetime import datetime


class Base_API:
    def __init__(self):
        pass

    def get_HidroSerieChuva(self, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieChuva/v1"
        return self._get_hidroserie_generica(base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieCota(self, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieCotas/v1"
        return self._get_hidroserie_generica(base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieVazao(self, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieVazao/v1"
        return self._get_hidroserie_generica(base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)
    
    def get_HidroSerieCurvaDescarga(self, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieCurvaDescarga/v1"
        return self._get_hidroserie_generica(base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSeriePerfilTransversal(self, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSeriePerfilTransversal/v1"
        return self._get_hidroserie_generica(base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieQA(self, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieQA/v1"
        return self._get_hidroserie_generica(base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)
    
    def get_HidroSerieResumoDescarga(self, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieResumoDescarga/v1"
        return self._get_hidroserie_generica(base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)
    
    def get_HidroSerieSedimentos(self, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieSedimentos/v1"
        return self._get_hidroserie_generica(base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)
    
    def get_HidroSerieDados(self, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieDados/v1"
        return self._get_hidroserie_generica(base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def _get_hidroserie_generica(self, base_url, token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final):
        try:
            dt_inicial = datetime.strptime(data_inicial, '%Y-%m-%d')
            dt_final = datetime.strptime(data_final, '%Y-%m-%d')
            if (dt_final - dt_inicial).days > 366:
                raise ValueError("O intervalo de busca não pode ser maior que 366 dias")
            if dt_final < dt_inicial:
                raise ValueError("A data final deve ser maior ou igual à data inicial")
        except ValueError as e:
            raise ValueError(f"Formato de data inválido ou problema no intervalo: {str(e)}")

        if tipo_filtro_data not in ['DATA_LEITURA', 'DATA_ULTIMA_ATUALIZACAO']:
            raise ValueError("Tipo de filtro de data deve ser 'DATA_LEITURA' ou 'DATA_ULTIMA_ATUALIZACAO'")

        params = {
            'Código da Estação': codigo_da_estacao,
            'Tipo Filtro Data': tipo_filtro_data,
            'Data Inicial (yyyy-MM-dd)': data_inicial,
            'Data Final (yyyy-MM-dd)': data_final
        }

        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code == 401:
            raise ValueError("TOKEN_INVALIDO")

        response.raise_for_status()
        return response.json()

    def get_HidroInventarioEstacoes(self, token, **kwargs):
        base_url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroInventarioEstacoes/v1"
        param_mapping = {
            'codigo_estacao': 'Código da Estação',
            'data_atualizacao_inicial': 'Data Atualização Inicial (yyyy-MM-dd)',
            'data_atualizacao_final': 'Data Atualização Final (yyyy-MM-dd)',
            'uf': 'Unidade Federativa',
            'codigo_bacia': 'Código da Bacia'
        }

        if not kwargs:
            raise ValueError("Pelo menos um parâmetro de filtro deve ser fornecido")

        params = {
            param_mapping[key]: value
            for key, value in kwargs.items() if key in param_mapping and value is not None
        }

        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code == 401:
            raise ValueError("TOKEN_INVALIDO")

        response.raise_for_status()
        return response.json()

    def get_HidroinfoanaSerieTelemetricaDetalhada(self, token, codigo_da_estacao, tipo_filtro_data,
                                                   data_de_busca, range_intervalo_de_busca):
        url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroinfoanaSerieTelemetricaDetalhada/v1"
        return self._get_telem_generica(url, token, codigo_da_estacao, tipo_filtro_data,
                                        data_de_busca, range_intervalo_de_busca)

    def get_HidroinfoanaSerieTelemetricaAdotada(self, token, codigo_da_estacao, tipo_filtro_data,
                                                data_de_busca, range_intervalo_de_busca):
        url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroinfoanaSerieTelemetricaAdotada/v1"
        return self._get_telem_generica(url, token, codigo_da_estacao, tipo_filtro_data,
                                        data_de_busca, range_intervalo_de_busca)

    def _get_telem_generica(self, base_url, token, codigo_da_estacao, tipo_filtro_data,
                            data_de_busca, range_intervalo_de_busca):
        try:
            dias = int(range_intervalo_de_busca.split('_')[1])
            if dias > 30:
                raise ValueError("O intervalo de busca não pode ser maior que 30 dias")
        except Exception:
            raise ValueError("Range de intervalo inválido. Use o formato 'DIAS_X' com X até 30")

        try:
            datetime.strptime(data_de_busca, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Formato de data inválido. Use 'yyyy-MM-dd'")

        params = {
            'Código da Estação': codigo_da_estacao,
            'Tipo Filtro Data': tipo_filtro_data,
            'Data de Busca (yyyy-MM-dd)': data_de_busca,
            'Range Intervalo de busca': range_intervalo_de_busca
        }

        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code == 401:
            raise ValueError("TOKEN_INVALIDO")

        response.raise_for_status()
        return response.json()
