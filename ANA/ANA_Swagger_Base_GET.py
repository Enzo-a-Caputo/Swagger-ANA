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

from __future__ import annotations

from datetime import datetime
from typing import Any

import requests


class Base_API:
    _BASE_URL = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas"
    _TIMEOUT  = 60

    def __init__(self) -> None:
        self._session = requests.Session()

    def __enter__(self) -> Base_API:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Fecha a sessão HTTP subjacente."""
        self._session.close()

    # ------------------------------------------------------------------
    # Séries convencionais (limite 366 dias por requisição)
    # ------------------------------------------------------------------
    def get_HidroSerieChuva(self, token: str, codigo_da_estacao: int, tipo_filtro_data: str,
                            data_inicial: str, data_final: str) -> dict[str, Any]:
        return self._get_hidroserie_generica(
            f"{self._BASE_URL}/HidroSerieChuva/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieCota(self, token: str, codigo_da_estacao: int, tipo_filtro_data: str,
                           data_inicial: str, data_final: str) -> dict[str, Any]:
        return self._get_hidroserie_generica(
            f"{self._BASE_URL}/HidroSerieCotas/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieVazao(self, token: str, codigo_da_estacao: int, tipo_filtro_data: str,
                            data_inicial: str, data_final: str) -> dict[str, Any]:
        return self._get_hidroserie_generica(
            f"{self._BASE_URL}/HidroSerieVazao/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieCurvaDescarga(self, token: str, codigo_da_estacao: int, tipo_filtro_data: str,
                                    data_inicial: str, data_final: str) -> dict[str, Any]:
        return self._get_hidroserie_generica(
            f"{self._BASE_URL}/HidroSerieCurvaDescarga/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSeriePerfilTransversal(self, token: str, codigo_da_estacao: int, tipo_filtro_data: str,
                                        data_inicial: str, data_final: str) -> dict[str, Any]:
        return self._get_hidroserie_generica(
            f"{self._BASE_URL}/HidroSeriePerfilTransversal/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieQA(self, token: str, codigo_da_estacao: int, tipo_filtro_data: str,
                         data_inicial: str, data_final: str) -> dict[str, Any]:
        return self._get_hidroserie_generica(
            f"{self._BASE_URL}/HidroSerieQA/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieResumoDescarga(self, token: str, codigo_da_estacao: int, tipo_filtro_data: str,
                                     data_inicial: str, data_final: str) -> dict[str, Any]:
        return self._get_hidroserie_generica(
            f"{self._BASE_URL}/HidroSerieResumoDescarga/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieSedimentos(self, token: str, codigo_da_estacao: int, tipo_filtro_data: str,
                                 data_inicial: str, data_final: str) -> dict[str, Any]:
        return self._get_hidroserie_generica(
            f"{self._BASE_URL}/HidroSerieSedimentos/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    def get_HidroSerieGranulometria(self, token: str, codigo_da_estacao: int, tipo_filtro_data: str,
                                    data_inicial: str, data_final: str) -> dict[str, Any]:
        return self._get_hidroserie_generica(
            f"{self._BASE_URL}/HidroSerieGranulometria/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_inicial, data_final)

    # ------------------------------------------------------------------
    # Séries telemétricas (limite 30 dias por requisição)
    # ------------------------------------------------------------------
    def get_HidroinfoanaSerieTelemetricaDetalhada(self, token: str, codigo_da_estacao: int,
                                                  tipo_filtro_data: str, data_de_busca: str,
                                                  range_intervalo_de_busca: str) -> dict[str, Any]:
        return self._get_telem_generica(
            f"{self._BASE_URL}/HidroinfoanaSerieTelemetricaDetalhada/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_de_busca, range_intervalo_de_busca)

    def get_HidroinfoanaSerieTelemetricaAdotada(self, token: str, codigo_da_estacao: int,
                                                tipo_filtro_data: str, data_de_busca: str,
                                                range_intervalo_de_busca: str) -> dict[str, Any]:
        return self._get_telem_generica(
            f"{self._BASE_URL}/HidroinfoanaSerieTelemetricaAdotada/v1",
            token, codigo_da_estacao, tipo_filtro_data, data_de_busca, range_intervalo_de_busca)

    # ------------------------------------------------------------------
    # Inventário e listas
    # ------------------------------------------------------------------
    def get_HidroInventarioEstacoes(self, token: str, **kwargs: Any) -> dict[str, Any]:
        param_mapping = {
            'codigo_estacao':            'Código da Estação',
            'data_atualizacao_inicial':  'Data Atualização Inicial (yyyy-MM-dd)',
            'data_atualizacao_final':    'Data Atualização Final (yyyy-MM-dd)',
            'uf':                        'Unidade Federativa',
            'codigo_bacia':              'Código da Bacia',
        }
        if not kwargs:
            raise ValueError("Pelo menos um parâmetro de filtro deve ser fornecido")

        params = {
            param_mapping[k]: v
            for k, v in kwargs.items() if k in param_mapping and v is not None
        }
        return self._request(f"{self._BASE_URL}/HidroInventarioEstacoes/v1", token, params)

    # ------------------------------------------------------------------
    # Helpers privados
    # ------------------------------------------------------------------
    def _request(self, url: str, token: str, params: dict[str, Any]) -> dict[str, Any]:
        """Executa o GET com session/timeout compartilhados e normaliza erros de autenticação."""
        headers = {'Authorization': f'Bearer {token}'}
        response = self._session.get(url, headers=headers, params=params, timeout=self._TIMEOUT)
        if response.status_code == 401:
            raise ValueError("TOKEN_INVALIDO")
        response.raise_for_status()
        return response.json()

    def _get_hidroserie_generica(self, url: str, token: str, codigo_da_estacao: int,
                                 tipo_filtro_data: str, data_inicial: str,
                                 data_final: str) -> dict[str, Any]:
        try:
            dt_ini = datetime.strptime(data_inicial, '%Y-%m-%d')
            dt_fim = datetime.strptime(data_final,   '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(f"Formato de data inválido (use yyyy-MM-dd): {e}") from e

        if dt_fim < dt_ini:
            raise ValueError("A data final deve ser maior ou igual à data inicial")
        if (dt_fim - dt_ini).days > 366:
            raise ValueError("O intervalo de busca não pode ser maior que 366 dias")
        if tipo_filtro_data not in ('DATA_LEITURA', 'DATA_ULTIMA_ATUALIZACAO'):
            raise ValueError("Tipo de filtro de data deve ser 'DATA_LEITURA' ou 'DATA_ULTIMA_ATUALIZACAO'")

        params = {
            'Código da Estação':         codigo_da_estacao,
            'Tipo Filtro Data':          tipo_filtro_data,
            'Data Inicial (yyyy-MM-dd)': data_inicial,
            'Data Final (yyyy-MM-dd)':   data_final,
        }
        return self._request(url, token, params)

    def _get_telem_generica(self, url: str, token: str, codigo_da_estacao: int,
                            tipo_filtro_data: str, data_de_busca: str,
                            range_intervalo_de_busca: str) -> dict[str, Any]:
        try:
            dias = int(range_intervalo_de_busca.split('_')[1])
        except (IndexError, ValueError) as e:
            raise ValueError("Range de intervalo inválido. Use o formato 'DIAS_X' com X até 30") from e
        if dias > 30:
            raise ValueError("O intervalo de busca não pode ser maior que 30 dias")

        try:
            datetime.strptime(data_de_busca, '%Y-%m-%d')
        except ValueError as e:
            raise ValueError("Formato de data inválido. Use 'yyyy-MM-dd'") from e

        params = {
            'Código da Estação':          codigo_da_estacao,
            'Tipo Filtro Data':           tipo_filtro_data,
            'Data de Busca (yyyy-MM-dd)': data_de_busca,
            'Range Intervalo de busca':   range_intervalo_de_busca,
        }
        return self._request(url, token, params)
