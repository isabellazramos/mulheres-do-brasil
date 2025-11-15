<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# gostaria que voce me fornecesse as documentacoes das apis/lib para eu saber quais informações eu consigo pegar

Perfeito! Vou compilar para você um **guia completo de documentação** das principais APIs e bibliotecas para dados brasileiros sobre mulheres, desigualdade e vulnerabilidade social:

***

# 📚 **Documentação Completa - APIs e Bibliotecas para Dados Brasileiros**

## **1. SIDRA API (IBGE) - Sistema IBGE de Recuperação Automática**

### **📖 Links Oficiais:**

- **API Oficial**: http://api.sidra.ibge.gov.br
- **Documentação API**: http://api.sidra.ibge.gov.br/home/ajuda
- **Portal SIDRA**: https://sidra.ibge.gov.br
- **Novo SIDRA** (interface moderna): https://sidra.ibge.gov.br[^1]


### **🐍 Biblioteca Python: sidrapy**

- **Documentação**: https://sidrapy.readthedocs.io[^2][^3]
- **PyPI**: https://pypi.org/project/sidrapy/[^3]
- **Instalação**: `pip install -U sidrapy`


### **📊 Principais Tabelas Relevantes:**

| Código | Nome | Dados Disponíveis |
| :-- | :-- | :-- |
| **6403** | Rendimento médio PNAD | Rendimento por trimestre (Brasil/Estados) |
| **5434** | Rendimento mensal real | Rendimento trabalho principal |
| **6381** | Taxa de desocupação | Desemprego por sexo e região |
| **1620** | Rendimento médio (antiga) | Rendimento habitual e efetivo |
| **6**662 | PNAD Contínua - Educação | Escolaridade por sexo |
| **3913** | Estatísticas de Gênero | Indicadores específicos mulheres |

### **💡 Exemplo de Uso:**

```python
import sidrapy

# Buscar rendimento médio
df = sidrapy.get_table(
    table_code="6403",
    territorial_level="1",  # 1=Brasil, 2=Região, 3=UF
    ibge_territorial_code="all",
    period="last 12"  # Últimos 12 trimestres
)
```


### **🔗 Estrutura da URL da API:**

```
https://apisidra.ibge.gov.br/values/t/{tabela}/n{nivel}/{territorio}/v/{variavel}/p/{periodo}/c{classificacao}/{categorias}
```

**Parâmetros:**

- `t` = Código da tabela
- `n` = Nível territorial (1=Brasil, 2=Região, 3=UF, 6=Município)
- `v` = Código da variável
- `p` = Período (ex: `last 12`, `202301-202412`)
- `c` = Classificação (ex: c2=Sexo, c58=Sexo)

***

## **2. PySUS - Dados do DataSUS (Saúde)**

### **📖 Links Oficiais:**

- **Documentação**: https://pysus.readthedocs.io[^4][^5][^6]
- **GitHub**: https://github.com/AlertaDengue/PySUS
- **Instalação**: `pip install pysus`


### **🏥 Sistemas Disponíveis:**

| Sistema | Descrição | Dados |
| :-- | :-- | :-- |
| **SINAN** | Notificações de agravos | Violência, dengue, tuberculose, etc[^4] |
| **SIM** | Mortalidade | Óbitos por causas |
| **SINASC** | Nascidos vivos | Dados de nascimento |
| **SIH** | Internações hospitalares | AIH, procedimentos |
| **CNES** | Cadastro estabelecimentos | Estrutura da saúde |

### **💡 Exemplo - SINAN (Violência):**

```python
from pysus.online_data import SINAN

# Baixar dados de violência
sinan = SINAN()

# Listar arquivos disponíveis
arquivos = sinan.get_files('VIOL', 2023)  # Violência 2023

# Download
parquet = sinan.download(arquivos)

# Converter para DataFrame
df = parquet.to_dataframe()
```


### **📋 Metadados SINAN:**

```python
# Ver estrutura dos dados
metadata = SINAN.metadata_df('VIOL')
print(metadata[['Nome do campo', 'Descrição']])
```

**Campos importantes para violência contra mulheres:**

- `CS_SEXO`: Sexo da vítima
- `TP_VIOLENC`: Tipo de violência
- `ID_MUNICIP`: Município
- `DT_NOTIFIC`: Data da notificação

***

## **3. Base dos Dados - Repositório Público**

### **📖 Links Oficiais:**

- **Site**: https://basedosdados.org
- **Documentação**: https://basedosdados.github.io/mais/
- **GitHub**: https://github.com/basedosdados/mais


### **🗂️ Datasets Relevantes:**

- **PNAD Contínua**: Mercado de trabalho, renda
- **SINAN**: Notificações de saúde
- **Censo Demográfico**: População completa
- **Atlas da Violência**: Dados compilados IPEA


### **💡 Uso via BigQuery (Google Cloud):**

```python
import basedosdados as bd

# Query SQL no BigQuery
query = """
SELECT ano, sexo, AVG(renda) as renda_media
FROM `basedosdados.br_ibge_pnad_continua.microdados`
WHERE ano >= 2020
GROUP BY ano, sexo
"""

df = bd.read_sql(query, billing_project_id="seu-projeto")
```


***

## **4. Dados Abertos Brasileiros - Portal do Governo**

### **📖 Links Oficiais:**

- **Portal**: https://dados.gov.br
- **API**: https://dados.gov.br/dados/api
- **Catálogo**: https://dados.gov.br/dados/conjuntos-dados


### **📊 Datasets Relevantes:**

- **RASEAM**: Relatório Anual Socioeconômico da Mulher
- **Bolsa Família**: Beneficiárias por município
- **Cadastro Único**: Famílias em vulnerabilidade
- **INFOPEN Mulheres**: População carcerária feminina


### **💡 Exemplo - Portal da Transparência API:**

```python
import requests

API_KEY = "sua_chave"  # Obter em http://api.portaldatransparencia.gov.br/

headers = {'chave-api-dados': API_KEY}

url = "http://api.portaldatransparencia.gov.br/api-de-dados/bolsa-familia-por-municipio"

params = {
    'mesAno': '202401',
    'codigoIbge': '3550308'  # São Paulo
}

response = requests.get(url, headers=headers, params=params)
dados = response.json()
```


***

## **5. DadosAbertosBrasil - Biblioteca Python**

### **📖 Links:**

- **PyPI**: https://pypi.org/project/DadosAbertosBrasil/[^7]
- **GitHub**: https://github.com/GusFurtado/DadosAbertosBrasil[^8]
- **Instalação**: `pip install DadosAbertosBrasil`


### **💡 Exemplo:**

```python
import DadosAbertosBrasil as dab

# Dados do IPEA
series_ipea = dab.ipea.list_series()

# Dados do Banco Central
dados_bc = dab.bacen.get_series(codigo='12')
```


***

## **6. Atlas da Violência (IPEA)**

### **📖 Links:**

- **Site**: http://www.ipea.gov.br/atlasviolencia
- **Dados**: https://www.ipea.gov.br/atlasviolencia/dados-series
- **API**: Dados disponíveis via download direto


### **📊 Indicadores:**

- Homicídios de mulheres por UF
- Feminicídios
- Violência doméstica
- Taxas por 100 mil habitantes

***

## **7. Recursos Adicionais**

### **🔗 Outras Fontes Úteis:**

| Fonte | URL | Dados |
| :-- | :-- | :-- |
| **IBGE Estatísticas de Gênero** | https://www.ibge.gov.br/estatisticas/multidominio/genero | Indicadores sociais mulheres |
| **Observatório da Violência** | https://observatorioseguranca.com.br | Segurança pública |
| **Brasil.IO** | https://brasil.io | Dados públicos compilados |
| **IPEA Data** | http://www.ipeadata.gov.br | Séries temporais |


***

## **📝 Resumo de Instalação:**

```bash
# Bibliotecas essenciais
pip install sidrapy pysus DadosAbertosBrasil pandas requests

# Opcional para análises
pip install matplotlib seaborn jupyter
```


***

## **🎯 Próximos Passos:**

Com essas documentações, você pode:

1. ✅ Explorar **tabelas disponíveis** no SIDRA
2. ✅ Baixar dados de **violência** via PySUS/SINAN
3. ✅ Acessar **indicadores sociais** compilados
4. ✅ Construir **datasets customizados** para seu projeto Spark

Quer que eu te ajude a explorar alguma dessas fontes especificamente ou começar a integrar com Spark? 🚀
<span style="display:none">[^10][^11][^12][^13][^14][^15][^16][^17][^18][^19][^20][^9]</span>

<div align="center">⁂</div>

[^1]: https://agenciadenoticias.ibge.gov.br/agencia-sala-de-imprensa/2013-agencia-de-noticias/releases/9481-novo-sidra-permite-consultar-facilmente-dados-de-estudos-e-pesquisas-do-ibge-tambem-em-dispositivos-moveis

[^2]: https://sidrapy.readthedocs.io

[^3]: https://pypi.org/project/sidrapy/

[^4]: https://pysus.readthedocs.io/pt/latest/tutorials/Preprocessing SINAN.html

[^5]: https://pysus.readthedocs.io/en/stable/tutorials/Dengue.html

[^6]: https://pysus.readthedocs.io/pt/stable/databases/SINAN.html

[^7]: https://pypi.org/project/DadosAbertosBrasil/

[^8]: https://github.com/GusFurtado/DadosAbertosBrasil

[^9]: http://pdf.blucher.com.br/marineengineeringproceedings/spolm2019/014.pdf

[^10]: http://www.scielo.br/pdf/bcg/v23n1/1982-2170-bcg-23-01-00150.pdf

[^11]: https://periodicos.ufpe.br/revistas/revistageografia/article/download/251781/40848

[^12]: http://www.scielo.br/pdf/rae/v34n2/a06v34n2.pdf

[^13]: https://www.scielo.br/j/rbmet/a/d6smph7RGwzv4d6XDnnh4kd/?format=pdf\&lang=pt

[^14]: http://www.scielo.br/pdf/bcg/v22n3/1982-2170-bcg-22-03-00389.pdf

[^15]: https://www.scielo.br/j/ci/a/Mbjn8vKDsBhS4MXC9LRpg7S/?format=pdf\&lang=pt

[^16]: https://www.scielo.br/j/ci/a/CK3c7G386LTxLwrHbGH4RYM/?format=pdf\&lang=pt

[^17]: https://pt.linkedin.com/pulse/download-de-dados-do-api-sidra-ibge-linguagem-python-martins-rq2of

[^18]: https://analisemacro.com.br/economia/indicadores/coletando-dados-do-sidra-com-o-python/

[^19]: https://sidrapy.readthedocs.io/pt-br/latest/modules/table.html

[^20]: https://lemate.paginas.ufsc.br/files/2020/08/Manual-SIDRA-versão-agosto-2020.pdf

