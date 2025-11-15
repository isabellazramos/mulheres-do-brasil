"""
Script 1: Teste básico de conexão com API
Objetivo: Verificar que conseguimos fazer requisições HTTP
"""

import requests
import json

def teste_conexao_simples():
    """Teste mais simples possível - buscar dados IBGE"""
    
    print("🔍 Iniciando teste de conexão...")
    
    # URL de exemplo: dados de população por UF
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    
    # Fazer a requisição
    resposta = requests.get(url)
    
    # Verificar se funcionou
    print(f"\n📡 Status da requisição: {resposta.status_code}")
    
    if resposta.status_code == 200:
        print("✅ Conexão bem-sucedida!\n")
        
        # Converter resposta para JSON
        dados = resposta.json()
        
        # Mostrar quantos estados temos
        print(f"📊 Total de UFs retornadas: {len(dados)}")
        
        # Mostrar primeiro estado como exemplo
        print("\n🔎 Exemplo do primeiro estado:")
        print(json.dumps(dados[0], indent=2, ensure_ascii=False))
        
        # Listar todos os estados
        print("\n📝 Lista de estados:")
        for estado in dados:
            print(f"  - {estado['sigla']}: {estado['nome']}")
        
        return dados
    else:
        print(f"❌ Erro na conexão: {resposta.status_code}")
        return None

# Executar o teste
if __name__ == "__main__":
    resultado = teste_conexao_simples()
    
    if resultado:
        print("\n🎉 Parabéns! Primeira conexão realizada com sucesso!")
   