import requests
from pprint import pprint
from datetime import datetime

def gerar_token_ana(identificador, senha):
    """
    Obtém o token de autenticação da API da ANA.
    Retorna uma tupla: (token JWT, validade datetime)
    """
    url = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/OAUth/v1"
    headers = {
        'Identificador': identificador,
        'Senha': senha
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        dados = response.json()
        print("🔹 Resposta da API:")
        pprint(dados)

        items = dados.get("items", {})
        token = items.get("tokenautenticacao")
        validade_str = items.get("validade")

        if not token:
            raise ValueError("Token não encontrado na resposta.")

        validade = None
        if validade_str:
            try:
                validade = datetime.strptime(validade_str, '%a %b %d %H:%M:%S GMT%z %Y')
            except Exception as e:
                print(f"⚠️ Erro ao converter validade: {e}")

        return token, validade

    except requests.RequestException as e:
        raise RuntimeError(f"Erro na requisição de autenticação: {e}")
    except ValueError as e:
        raise RuntimeError(f"Erro na resposta de autenticação: {e}")
