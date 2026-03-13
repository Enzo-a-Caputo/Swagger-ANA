# Swagger-ANA: Interface Hidro Webservice 🌊

Este repositório fornece uma biblioteca em Python para a automação do download e processamento de dados hidrometeorológicos através do **Hidro Webservice**, a nova API baseada em Swagger da Agência Nacional de Águas e Saneamento Básico (ANA).

O projeto transforma as requisições complexas da API em dados estruturados (Pandas DataFrames), prontos para aplicação em estudos de engenharia e recursos hídricos.

## 🏗️ Estrutura do Repositório

O projeto é modular para facilitar a manutenção e escalabilidade:

* `ANA_Swagger_Autenticacao.py`: Lógica de login e geração de tokens.
* `ANA_Swagger_Base_GET.py`: Métodos base para comunicação HTTP com o Swagger.
* `ANA_Swagger_Aplicacoes.py`: Funções de alto nível e ferramentas espaciais (Bacias/Mapas).
* `ANA_Swagger_Processamento.py`: Tratamento de dados brutos e exportação.
* `ANA_Swagger_Download.py`: Orquestração de downloads em lote por período.


### 1. Autenticação e Segurança (ANA_Swagger_Autenticacao.py)

O módulo `ANA_Swagger_Autenticacao.py` é o componente responsável pela gestão de acesso e segurança na comunicação com o Hidro Webservice. Ele implementa a função `gerar_token_ana`, que automatiza o processo de login enviando as credenciais do usuário via cabeçalhos HTTP para o endpoint de OAuth da Agência Nacional de Águas. Além de extrair o `tokenautenticacao` necessário para validar todas as requisições subsequentes de consulta e download, o script realiza o tratamento da string de validade retornada pela API, convertendo-a em um objeto `datetime` nativo do Python para permitir o controle programático da expiração da sessão. Vale ressaltar que o identificador e a senha necessários para o funcionamento deste módulo devem ser solicitados oficialmente através do portal do Hidroweb.

Aqui está o conteúdo formatado para o seu README.md, seguindo exatamente o estilo de texto corrido e técnico dos tópicos anteriores:


### 2. Comunicação Base com o Swagger (ANA_Swagger_Base_GET.py)

O módulo ANA_Swagger_Base_GET.py funciona como o motor de requisições do projeto, encapsulando a complexidade das chamadas HTTP na classe Base_API. Ele padroniza a comunicação com os diversos endpoints da ANA, gerenciando automaticamente os cabeçalhos de autenticação Bearer e os parâmetros de consulta necessários para cada operação. Uma característica fundamental deste módulo é a implementação de validações rigorosas antes do envio dos dados: o código verifica se os intervalos de datas respeitam o limite de 366 dias para séries históricas e 30 dias para dados telemétricos, além de validar o formato das strings e os tipos de filtros permitidos, como DATA_LEITURA ou DATA_ULTIMA_ATUALIZACAO. O módulo também trata respostas de erro específicas, como o status 401 para tokens expirados, e converte os retornos da API diretamente em dicionários JSON, servindo de base para o processamento de dados subsequente.

Abaixo estão listados todos os endpoints da ANA já implementados através deste módulo:

* `HidroSerieChuva`
* `HidroSerieCota`
* `HidroSerieVazao`
* `HidroSerieCurvaDescarga`
* `HidroSeriePerfilTransversal`
* `HidroSerieQA`
* `HidroSerieResumoDescarga`
* `HidroSerieSedimentos`
* `HidroSerieDados:`
* `HidroInventarioEstacoes`
* `HidroinfoanaSerieTelemetricaDetalhada`
* `HidroinfoanaSerieTelemetricaAdotada`


### 3. Downloads em Lote (ANA_Swagger_Download.py)
O módulo `ANA_Swagger_Download.py` implementa a classe `Download_JSON`, projetada para realizar a extração massiva de dados e o armazenamento em arquivos locais. Sua lógica principal resolve a limitação de intervalo da API através de loops temporais que particionam a solicitação por anos (séries históricas) ou janelas de 30 dias (telemetria), consolidando os itens retornados em um único arquivo JSON.

O componente inclui um mecanismo de persistência de sessão que utiliza a função `_verificar_e_renovar_token`. Este método monitora a validade do token JWT em tempo real e executa a renovação automática caso o tempo de expiração seja inferior a dois minutos, garantindo a continuidade de downloads de longa duração. O módulo também realiza o tratamento de erros HTTP 401, forçando a reautenticação imediata, e organiza os dados telemétricos por ordem cronológica após a filtragem do período exato solicitado.


### 4. Tratamento e Conversão de Dados (ANA_Swagger_Processamento.py)
O módulo ANA_Swagger_Processamento.py automatiza a transformação de arquivos JSON brutos em Pandas DataFrames e arquivos CSV estruturados. Através da classe Processamento_JSON, a biblioteca realiza o parsing de campos complexos, como a transposição de colunas mensais (ex: Chuva_01 a Chuva_31) para séries temporais contínuas, garantindo que cada linha represente uma única observação temporal.

As principais funcionalidades técnicas incluem:

* Normalização de Tipos: Conversão automática de strings para float e datetime, com tratamento de separadores decimais e remoção de registros inconsistentes.
* Processamento por Endpoint: Métodos dedicados para cada tipo de dado (Chuva, Vazão, Cota, Sedimentos, Qualidade da Água), que organizam automaticamente colunas críticas e níveis de consistência.
* Refino Telemétrico: Tratamento de dados horários e de alta resolução, com limpeza de strings e padronização de timestamps.
* Agregação Diária: Função extra `Agregar_Diario` para converter leituras intradiárias em séries diárias (soma para chuva e média/primeiro valor para níveis e vazões).
* Saída Estruturada: Exportação automática para CSV (separador ;) e retorno de dicionários contendo os DataFrames prontos para análise.


### 5. Ferramentas Espaciais e de Apoio (ANA_Swagger_Aplicacoes.py)
O módulo ANA_Swagger_Aplicacoes.py expande as capacidades da biblioteca ao oferecer funcionalidades que auxiliam o fluxo de trabalho geográfico do hidrólogo. Através da classe Aplicacoes, o módulo integra as bibliotecas GeoPandas, Matplotlib e Contextily para automatizar o inventário de estações dentro de áreas de interesse e a geração de mapas temáticos, permitindo a visualização espacial direta da bacia hidrográfica em estudo.

As funcionalidades de busca e visualização estão divididas em três níveis de complexidade:

* Busca e Recorte Básico (achar_estacoes_pela_bacia): Realiza o cruzamento espacial entre o polígono de uma bacia hidrográfica e a base de dados oficial da ANA. A função executa a reprojeção automática para o sistema de coordenadas WGS84 e o recorte (clip) das estações, retornando listas separadas de códigos para estações pluviométricas e fluviométricas. Se fornecida, plota uma representação básica da rede de drenagem.
* Visualização Avançada e Simbologia (achar_estacoes_pela_bacia_2): Aprimora a representação visual ao diferenciar as estações por simbologia técnica: triângulos verdes para pluviometria e círculos vermelhos para fluviometria. Esta função automatiza o ajuste de escala (tight_layout) e garante que o processamento da drenagem seja feito dentro do sistema de referência correto para evitar distorções espaciais.
* Contextualização Cartográfica (achar_estacoes_pela_bacia_3): É a ferramenta de maior nível técnico para apresentações e relatórios. Além do processamento espacial, ela utiliza a biblioteca contextily para adicionar um mapa de fundo (como OpenStreetMap ou imagens de satélite) ao gráfico. Para isso, a função realiza a reprojeção interna de todos os vetores para o sistema Mercator Global (EPSG:3857), permitindo o alinhamento perfeito entre os dados da ANA e os serviços de mapas web (basemaps).


## 🚀 Como Usar
O arquivo `ANA_Swagger_Exemplos.ipynb` serve como um guia prático para a implementação rápida das funcionalidades da biblioteca. Ele contém scripts pré-configurados que demonstram o fluxo completo de trabalho, desde a geração do token de acesso e o download automatizado de séries históricas até o processamento dos arquivos JSON em DataFrames e a exportação para CSV. Este módulo é ideal para novos usuários que desejam testar a comunicação com o Webservice da ANA ou integrar rapidamente as ferramentas de análise espacial em seus projetos de recursos hídricos.

---
### Pré-requisitos
```bash
pip install requests pandas geopandas matplotlib contextily
