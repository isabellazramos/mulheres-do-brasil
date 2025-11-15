"""
Script 2: Converter dados da API para DataFrame
Objetivo: Trabalhar com dados tabulares
"""

import requests
import pandas as pd

def buscar_municipios_uf(sigla_uf):
    """
    Busca todos os municípios de uma UF
    Exemplo: buscar_municipios_uf('SP')
    """
    
    print(f"🔍 Buscando municípios de {sigla_uf}...")
    
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{sigla_uf}/municipios"
    
    resposta = requests.get(url)
    
    if resposta.status_code == 200:
        dados = resposta.json()
        
        # Converter para DataFrame
        df = pd.DataFrame(dados)
        
        print(f"✅ {len(df)} municípios encontrados!")
        print(f"\n📊 Colunas disponíveis: {df.columns.tolist()}")
        print(f"\nPrimeiros 5 municípios:")
        print(df[['id', 'nome']].head())
        
        # Salvar em CSV
        arquivo = f"data/municipios_{sigla_uf}.csv"
        df.to_csv(arquivo, index=False, encoding='utf-8')
        print(f"\n💾 Dados salvos em: {arquivo}")
        
        return df
    else:
        print(f"❌ Erro: {resposta.status_code}")
        return None

# Executar para São Paulo
if __name__ == "__main__":
    df_sp = buscar_municipios_uf('SP')
    
    # Estatísticas básicas
    if df_sp is not None:
        print("\n📈 Estatísticas:")
        print(f"  Total de municípios: {len(df_sp)}")
        print(f"  Primeiro município: {df_sp.iloc[0]['nome']}")
        print(f"  Último município: {df_sp.iloc[-1]['nome']}")
