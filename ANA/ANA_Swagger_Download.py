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

import json
import logging
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Callable

import requests

from .ANA_Swagger_Base_GET import Base_API
from .ANA_Swagger_Autenticacao import gerar_token_ana


logger = logging.getLogger(__name__)

# Configura um handler padrão se a aplicação não configurou logging,
# para que mensagens apareçam em notebooks/scripts simples por default.
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )


class Download_JSON:
    _MAX_WORKERS           = 5
    _MAX_TENTATIVAS_PADRAO = 20
    _MAX_RENOVACOES_TOKEN  = 5
    _ESPERA_INICIAL_S      = 5
    _ESPERA_MAXIMA_S       = 300
    _STATUS_TRANSITORIOS   = (408, 425, 429, 500, 502, 503, 504)

    def __init__(self) -> None:
        self.Base               = Base_API()
        self._token: str | None = None
        self._token_lock        = threading.Lock()

        # Backoff global compartilhado entre workers
        self._backoff_lock      = threading.Lock()
        self._next_retry_after  = 0.0
        self._espera_atual      = self._ESPERA_INICIAL_S

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    def __enter__(self) -> Download_JSON:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Fecha a sessão HTTP subjacente."""
        self.Base.close()

    # ------------------------------------------------------------------
    # Autenticação thread-safe
    # ------------------------------------------------------------------
    def _obter_token(self, identificador: str, senha: str) -> str:
        """Retorna o token atual, gerando um novo se ainda não existir. Thread-safe."""
        with self._token_lock:
            if self._token is None:
                self._token, _ = gerar_token_ana(identificador, senha)
            return self._token

    def _renovar_token(self, identificador: str, senha, token_que_falhou: str) -> None:
        """
        Renova o token apenas se ainda for o mesmo que falhou.
        Se outra thread já renovou, simplesmente retorna. Thread-safe.
        """
        with self._token_lock:
            if self._token != token_que_falhou:
                return  # Outra thread já renovou; usa o token novo na próxima tentativa.
            logger.warning("Renovando token (expirado ou inválido)...")
            self._token, _ = gerar_token_ana(identificador, senha)

    # ------------------------------------------------------------------
    # Backoff global (compartilhado entre todos os workers)
    # ------------------------------------------------------------------
    def _aguardar_backoff_global(self) -> None:
        """Bloqueia até o instante de retry global ser atingido. Re-checa para janelas estendidas."""
        while True:
            restante = self._next_retry_after - time.monotonic()
            if restante <= 0:
                return
            time.sleep(restante)

    def _registrar_erro_transitorio(self, descricao: str) -> None:
        """
        Avança o backoff global. Apenas a primeira thread a reportar erro em cada
        janela aumenta o tempo de espera; as demais aproveitam a janela existente.
        """
        with self._backoff_lock:
            agora = time.monotonic()
            if agora < self._next_retry_after:
                return  # Já estamos em uma janela ativa; o backoff atual cobre esta falha.
            espera = self._espera_atual
            self._next_retry_after = agora + espera
            self._espera_atual = min(self._espera_atual * 2, self._ESPERA_MAXIMA_S)
            logger.warning(
                f"Erro transitório em '{descricao}'. Backoff global: {espera:.0f}s "
                f"(próxima espera: {self._espera_atual:.0f}s)."
            )

    def _resetar_backoff_global(self) -> None:
        """Após sucesso, retorna o backoff ao valor inicial."""
        if self._espera_atual == self._ESPERA_INICIAL_S:
            return  # Fast path sem lock
        with self._backoff_lock:
            self._espera_atual = self._ESPERA_INICIAL_S

    # ------------------------------------------------------------------
    # Retry centralizado
    # ------------------------------------------------------------------
    def _executar_com_retry(self, chamada: Callable[[str], dict[str, Any]],
                            identificador: str, senha: str,
                            max_tentativas: int | None = None,
                            descricao: str = "requisição") -> dict[str, Any]:
        """
        Executa `chamada(token)` com retry automático.

        - TOKEN_INVALIDO / HTTP 401: renova o token e retenta (sem consumir tentativa).
        - HTTP 408/425/429/5xx, ConnectionError, Timeout, ChunkedEncodingError,
          JSONDecodeError: usa backoff global compartilhado entre workers.
        - Demais erros: re-raise imediato.
        """
        if max_tentativas is None:
            max_tentativas = self._MAX_TENTATIVAS_PADRAO

        tentativa  = 0
        renovacoes = 0

        while True:
            self._aguardar_backoff_global()
            token_atual = self._obter_token(identificador, senha)

            try:
                resultado = chamada(token_atual)
                self._resetar_backoff_global()
                return resultado

            except ValueError as e:
                if str(e) != "TOKEN_INVALIDO":
                    raise
                renovacoes += 1
                if renovacoes > self._MAX_RENOVACOES_TOKEN:
                    raise RuntimeError(
                        f"Token rejeitado após {self._MAX_RENOVACOES_TOKEN} renovações "
                        f"em '{descricao}'."
                    ) from e
                self._renovar_token(identificador, senha, token_atual)

            except requests.exceptions.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status == 401:
                    renovacoes += 1
                    if renovacoes > self._MAX_RENOVACOES_TOKEN:
                        raise
                    self._renovar_token(identificador, senha, token_atual)
                    continue
                if status not in self._STATUS_TRANSITORIOS:
                    raise
                tentativa += 1
                if tentativa >= max_tentativas:
                    raise RuntimeError(
                        f"Falha em '{descricao}' após {max_tentativas} tentativas "
                        f"(HTTP {status})."
                    ) from e
                self._registrar_erro_transitorio(f"HTTP {status} em '{descricao}'")

            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError,
                    json.JSONDecodeError) as e:
                tentativa += 1
                if tentativa >= max_tentativas:
                    raise RuntimeError(
                        f"Falha em '{descricao}' após {max_tentativas} tentativas "
                        f"({type(e).__name__})."
                    ) from e
                self._registrar_erro_transitorio(f"{type(e).__name__} em '{descricao}'")

    # ------------------------------------------------------------------
    # Persistência
    # ------------------------------------------------------------------
    @staticmethod
    def _salvar_json_atomico(caminho: str, dados: Any) -> None:
        """Escreve em arquivo temporário e renomeia: evita JSON corrompido em caso de crash."""
        tmp = caminho + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
        os.replace(tmp, caminho)

    @staticmethod
    def _validar_anos(ano_inicial: int, ano_final: int) -> None:
        if ano_inicial >= ano_final:
            raise ValueError(
                f"ano_inicial ({ano_inicial}) deve ser menor que ano_final ({ano_final})."
            )

    # ------------------------------------------------------------------
    # Núcleo: download anual paralelo
    # ------------------------------------------------------------------
    def _baixar_serie_anual(self, identificador: str, senha: str, codigo_estacao: int,
                            pasta_saida: str,
                            fn_get: Callable[..., dict[str, Any]], prefixo: str,
                            tipo_filtro_data: str, ano_inicial: int, ano_final: int,
                            max_workers: int | None = None,
                            max_tentativas: int | None = None,
                            limpar_parciais: bool = False) -> str:
        """
        Baixa a série ano-a-ano em paralelo.

        Cada ano é salvo em `.parciais_<prefixo>_estacao_<cod>/<ano>.json` para
        permitir resume: re-executar pula anos já baixados.
        Ao final, consolida tudo em `<prefixo>_estacao_<cod>.json`.

        Se `limpar_parciais=True`, remove a pasta de parciais após o merge.
        """
        self._validar_anos(ano_inicial, ano_final)
        os.makedirs(pasta_saida, exist_ok=True)
        pasta_parciais = os.path.join(
            pasta_saida, f".parciais_{prefixo}_estacao_{codigo_estacao}"
        )
        os.makedirs(pasta_parciais, exist_ok=True)

        if max_workers is None:
            max_workers = self._MAX_WORKERS

        todos_anos     = list(range(ano_inicial, ano_final))
        anos_pendentes = [
            ano for ano in todos_anos
            if not os.path.exists(os.path.join(pasta_parciais, f"{ano}.json"))
        ]
        pulados = len(todos_anos) - len(anos_pendentes)
        if pulados:
            logger.info(f"[{prefixo}] {pulados} anos já baixados. {len(anos_pendentes)} pendentes.")
        if not anos_pendentes:
            logger.info(f"[{prefixo}] Todos os anos já baixados.")
        else:
            logger.info(f"[{prefixo}] Baixando {len(anos_pendentes)} anos "
                        f"com {max_workers} workers paralelos...")

            def baixar_ano(ano: int) -> None:
                data_ini = f"{ano}-01-01"
                data_fim = f"{ano + 1}-01-01"

                def chamada(token: str, _ini: str = data_ini, _fim: str = data_fim) -> dict[str, Any]:
                    return fn_get(token, codigo_estacao, tipo_filtro_data, _ini, _fim)

                resultado = self._executar_com_retry(
                    chamada, identificador, senha,
                    max_tentativas=max_tentativas,
                    descricao=f"{prefixo} estação {codigo_estacao} ano {ano}",
                )
                itens = resultado.get("items", []) if isinstance(resultado, dict) else []
                self._salvar_json_atomico(
                    os.path.join(pasta_parciais, f"{ano}.json"), itens
                )
                logger.info(f"[{prefixo}] {ano}: {len(itens)} itens.")

            erros: list[tuple[int, BaseException]] = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(baixar_ano, ano): ano for ano in anos_pendentes}
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        ano = futures[future]
                        logger.error(f"[{prefixo}] Erro no ano {ano}: {e}")
                        erros.append((ano, e))

            if erros:
                anos_str = ", ".join(str(a) for a, _ in sorted(erros))
                raise RuntimeError(
                    f"[{prefixo}] Falha em {len(erros)} ano(s): {anos_str}. "
                    "Re-execute para tentar novamente os pendentes."
                )

        dados_todos: list[Any] = []
        for ano in todos_anos:
            with open(os.path.join(pasta_parciais, f"{ano}.json"), "r", encoding="utf-8") as f:
                dados_todos.extend(json.load(f))

        caminho_final = os.path.join(pasta_saida, f"{prefixo}_estacao_{codigo_estacao}.json")
        self._salvar_json_atomico(caminho_final, dados_todos)
        logger.info(f"[{prefixo}] JSON consolidado: {caminho_final} ({len(dados_todos)} registros).")

        if limpar_parciais:
            shutil.rmtree(pasta_parciais, ignore_errors=True)
            logger.info(f"[{prefixo}] Parciais removidos.")

        return caminho_final

    # ------------------------------------------------------------------
    # Wrappers públicos
    # ------------------------------------------------------------------
    def D_HidroSerieChuva(self, identificador: str, senha: str, codigo_estacao: int,
                          pasta_saida: str, tipo_filtro_data: str = 'DATA_LEITURA',
                          ano_inicial: int = 1900, ano_final: int = 2025,
                          max_workers: int | None = None,
                          limpar_parciais: bool = False) -> str:
        return self._baixar_serie_anual(
            identificador, senha, codigo_estacao, pasta_saida,
            fn_get=self.Base.get_HidroSerieChuva, prefixo="chuva",
            tipo_filtro_data=tipo_filtro_data, ano_inicial=ano_inicial, ano_final=ano_final,
            max_workers=max_workers, limpar_parciais=limpar_parciais,
        )

    def D_HidroSerieCota(self, identificador: str, senha: str, codigo_estacao: int,
                         pasta_saida: str, tipo_filtro_data: str = 'DATA_LEITURA',
                         ano_inicial: int = 1900, ano_final: int = 2025,
                         max_workers: int | None = None,
                         limpar_parciais: bool = False) -> str:
        return self._baixar_serie_anual(
            identificador, senha, codigo_estacao, pasta_saida,
            fn_get=self.Base.get_HidroSerieCota, prefixo="Cota",
            tipo_filtro_data=tipo_filtro_data, ano_inicial=ano_inicial, ano_final=ano_final,
            max_workers=max_workers, limpar_parciais=limpar_parciais,
        )

    def D_HidroSerieVazao(self, identificador: str, senha: str, codigo_estacao: int,
                          pasta_saida: str, tipo_filtro_data: str = 'DATA_LEITURA',
                          ano_inicial: int = 1900, ano_final: int = 2025,
                          max_workers: int | None = None,
                          limpar_parciais: bool = False) -> str:
        return self._baixar_serie_anual(
            identificador, senha, codigo_estacao, pasta_saida,
            fn_get=self.Base.get_HidroSerieVazao, prefixo="Vazao",
            tipo_filtro_data=tipo_filtro_data, ano_inicial=ano_inicial, ano_final=ano_final,
            max_workers=max_workers, limpar_parciais=limpar_parciais,
        )

    def D_HidroSerieCurvaDescarga(self, identificador: str, senha: str, codigo_estacao: int,
                                  pasta_saida: str, tipo_filtro_data: str = 'DATA_LEITURA',
                                  ano_inicial: int = 1900, ano_final: int = 2025,
                                  max_workers: int | None = None,
                                  limpar_parciais: bool = False) -> str:
        return self._baixar_serie_anual(
            identificador, senha, codigo_estacao, pasta_saida,
            fn_get=self.Base.get_HidroSerieCurvaDescarga, prefixo="CurvaDescarga",
            tipo_filtro_data=tipo_filtro_data, ano_inicial=ano_inicial, ano_final=ano_final,
            max_workers=max_workers, limpar_parciais=limpar_parciais,
        )

    def D_HidroSeriePerfilTransversal(self, identificador: str, senha: str, codigo_estacao: int,
                                      pasta_saida: str, tipo_filtro_data: str = 'DATA_LEITURA',
                                      ano_inicial: int = 1900, ano_final: int = 2025,
                                      max_workers: int | None = None,
                                      limpar_parciais: bool = False) -> str:
        return self._baixar_serie_anual(
            identificador, senha, codigo_estacao, pasta_saida,
            fn_get=self.Base.get_HidroSeriePerfilTransversal, prefixo="PerfilTransversal",
            tipo_filtro_data=tipo_filtro_data, ano_inicial=ano_inicial, ano_final=ano_final,
            max_workers=max_workers, limpar_parciais=limpar_parciais,
        )

    def D_HidroSerieQA(self, identificador: str, senha: str, codigo_estacao: int,
                       pasta_saida: str, tipo_filtro_data: str = 'DATA_LEITURA',
                       ano_inicial: int = 1900, ano_final: int = 2025,
                       max_workers: int | None = None,
                       limpar_parciais: bool = False) -> str:
        return self._baixar_serie_anual(
            identificador, senha, codigo_estacao, pasta_saida,
            fn_get=self.Base.get_HidroSerieQA, prefixo="QA",
            tipo_filtro_data=tipo_filtro_data, ano_inicial=ano_inicial, ano_final=ano_final,
            max_workers=max_workers, limpar_parciais=limpar_parciais,
        )

    def D_HidroSerieResumoDescarga(self, identificador: str, senha: str, codigo_estacao: int,
                                   pasta_saida: str, tipo_filtro_data: str = 'DATA_LEITURA',
                                   ano_inicial: int = 1900, ano_final: int = 2025,
                                   max_workers: int | None = None,
                                   limpar_parciais: bool = False) -> str:
        return self._baixar_serie_anual(
            identificador, senha, codigo_estacao, pasta_saida,
            fn_get=self.Base.get_HidroSerieResumoDescarga, prefixo="ResumoDescarga",
            tipo_filtro_data=tipo_filtro_data, ano_inicial=ano_inicial, ano_final=ano_final,
            max_workers=max_workers, limpar_parciais=limpar_parciais,
        )

    def D_HidroSerieSedimentos(self, identificador: str, senha: str, codigo_estacao: int,
                               pasta_saida: str, tipo_filtro_data: str = 'DATA_LEITURA',
                               ano_inicial: int = 1900, ano_final: int = 2025,
                               max_workers: int | None = None,
                               limpar_parciais: bool = False) -> str:
        return self._baixar_serie_anual(
            identificador, senha, codigo_estacao, pasta_saida,
            fn_get=self.Base.get_HidroSerieSedimentos, prefixo="Sedimentos",
            tipo_filtro_data=tipo_filtro_data, ano_inicial=ano_inicial, ano_final=ano_final,
            max_workers=max_workers, limpar_parciais=limpar_parciais,
        )

    def D_HidroSerieGranulometria(self, identificador: str, senha: str, codigo_estacao: int,
                                  pasta_saida: str, tipo_filtro_data: str = 'DATA_LEITURA',
                                  ano_inicial: int = 1900, ano_final: int = 2025,
                                  max_workers: int | None = None,
                                  limpar_parciais: bool = False) -> str:
        return self._baixar_serie_anual(
            identificador, senha, codigo_estacao, pasta_saida,
            fn_get=self.Base.get_HidroSerieGranulometria, prefixo="Granulometria",
            tipo_filtro_data=tipo_filtro_data, ano_inicial=ano_inicial, ano_final=ano_final,
            max_workers=max_workers, limpar_parciais=limpar_parciais,
        )

    # ------------------------------------------------------------------
    # Telemétrica detalhada (varredura em janelas de 30 dias para trás)
    # ------------------------------------------------------------------
    def D_HidroinfoanaSerieTelemetricaDetalhada(
        self, identificador: str, senha: str, codigo_estacao: int, pasta_saida: str,
        tipo_filtro_data: str = 'DATA_LEITURA',
        ano_inicial: int = 1900, ano_final: int = 2025,
        intervalo_dias: str = "DIAS_30",
        max_workers: int | None = None,
        limpar_parciais: bool = False,
    ) -> str:
        """
        Baixa dados telemétricos detalhados varrendo o período de trás para frente
        em janelas de até 30 dias, em paralelo.
        Cada janela é salva como arquivo parcial, permitindo retomar do ponto
        interrompido em uma re-execução.
        """
        self._validar_anos(ano_inicial, ano_final)
        os.makedirs(pasta_saida, exist_ok=True)
        pasta_parciais = os.path.join(
            pasta_saida, f".parciais_telemetria_estacao_{codigo_estacao}"
        )
        os.makedirs(pasta_parciais, exist_ok=True)

        if max_workers is None:
            max_workers = self._MAX_WORKERS

        try:
            dias_intervalo = int(intervalo_dias.split("_")[1])
        except (IndexError, ValueError) as e:
            raise ValueError("intervalo_dias deve ter o formato 'DIAS_X' (ex.: 'DIAS_30').") from e

        data_inicio = datetime(ano_inicial, 1, 1) - timedelta(days=30)
        data_fim    = datetime(ano_final, 12, 31) + timedelta(days=30)

        # Enumera todas as janelas do período
        janelas: list[tuple[datetime, datetime]] = []
        data_atual = data_fim
        while data_atual >= data_inicio:
            inicio_janela = max(data_atual - timedelta(days=dias_intervalo - 1), data_inicio)
            janelas.append((data_atual, inicio_janela))
            data_atual = inicio_janela - timedelta(days=1)

        janelas_pendentes = [
            (da, ij) for da, ij in janelas
            if not os.path.exists(
                os.path.join(pasta_parciais, f"{da.strftime('%Y-%m-%d')}.json")
            )
        ]
        puladas = len(janelas) - len(janelas_pendentes)
        if puladas:
            logger.info(f"[telemetria] {puladas} janelas já baixadas. "
                        f"{len(janelas_pendentes)} pendentes.")

        logger.info(f"[telemetria] Estação {codigo_estacao}: {ano_inicial}-{ano_final} "
                    f"({len(janelas)} janelas de {dias_intervalo} dias, "
                    f"{max_workers} workers paralelos).")

        if janelas_pendentes:
            concluidas    = puladas
            total_janelas = len(janelas)

            def baixar_janela(data_atual: datetime, inicio_janela: datetime) -> tuple[str, int]:
                data_str = data_atual.strftime("%Y-%m-%d")

                def chamada(token: str, _data: str = data_str) -> dict[str, Any]:
                    return self.Base.get_HidroinfoanaSerieTelemetricaDetalhada(
                        token, codigo_estacao, tipo_filtro_data,
                        _data, intervalo_dias,
                    )

                resultado = self._executar_com_retry(
                    chamada, identificador, senha,
                    descricao=f"telemetria estação {codigo_estacao} janela {data_str}",
                )
                itens = resultado.get("items", []) if isinstance(resultado, dict) else []
                self._salvar_json_atomico(
                    os.path.join(pasta_parciais, f"{data_str}.json"), itens
                )
                return data_str, len(itens)

            erros: list[tuple[datetime, BaseException]] = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(baixar_janela, da, ij): da
                    for da, ij in janelas_pendentes
                }
                for future in as_completed(futures):
                    da = futures[future]
                    try:
                        data_str, n_itens = future.result()
                        concluidas += 1
                        progresso = concluidas / total_janelas * 100
                        logger.info(f"[telemetria] {progresso:5.1f}% – janela {data_str}: "
                                    f"{n_itens} registros.")
                    except Exception as e:
                        logger.error(f"[telemetria] Erro na janela {da.strftime('%Y-%m-%d')}: {e}")
                        erros.append((da, e))

            if erros:
                datas_str = ", ".join(da.strftime("%Y-%m-%d") for da, _ in sorted(erros))
                raise RuntimeError(
                    f"[telemetria] Falha em {len(erros)} janela(s): {datas_str}. "
                    "Re-execute para tentar novamente as pendentes."
                )

        # Merge de todos os parciais + filtragem pelo período exato
        dados_coletados: list[Any] = []
        for nome in sorted(os.listdir(pasta_parciais)):
            if not nome.endswith(".json"):
                continue
            with open(os.path.join(pasta_parciais, nome), "r", encoding="utf-8") as f:
                dados_coletados.extend(json.load(f))

        data_inicio_real = datetime(ano_inicial, 1, 1)
        data_fim_real    = datetime(ano_final, 12, 31, 23, 59, 59)

        def parse_data(item: dict[str, Any]) -> datetime:
            return datetime.strptime(item["Data_Hora_Medicao"], "%Y-%m-%d %H:%M:%S.%f")

        dados_filtrados: list[dict[str, Any]] = []
        for item in dados_coletados:
            try:
                d = parse_data(item)
            except (KeyError, ValueError, TypeError):
                continue
            if data_inicio_real <= d <= data_fim_real:
                dados_filtrados.append(item)

        dados_filtrados.sort(key=parse_data)

        caminho_final = os.path.join(pasta_saida, f"telemetria_estacao_{codigo_estacao}.json")
        self._salvar_json_atomico(caminho_final, dados_filtrados)

        logger.info(f"[telemetria] Concluído: {len(dados_coletados)} registros brutos, "
                    f"{len(dados_filtrados)} dentro do período.")
        logger.info(f"[telemetria] JSON salvo em {caminho_final}")

        if limpar_parciais:
            shutil.rmtree(pasta_parciais, ignore_errors=True)
            logger.info("[telemetria] Parciais removidos.")

        return caminho_final
