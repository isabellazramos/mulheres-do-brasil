"""
Usando sidrapy: biblioteca Python específica para IBGE SIDRA
Muito mais fácil que fazer requests diretos!
"""
import sidrapy
import pandas as pd

def buscar_rendimento_por_sexo():
    """
    Busca dados da PNAD Contínua - Tabela 6403
    Sem especificar variável (pega tudo disponível)
    """
    print("🔍 Buscando dados da PNAD Contínua...\n")
    
    df = sidrapy.get_table(
        table_code="6403",
        territorial_level="1",
        ibge_territorial_code="all",
        period="last 8"  # Últimos 8 trimestres
    )
    
    print(f"✅ Dados baixados! Shape: {df.shape}\n")
    
    # Remover linha de cabeçalho (primeira linha)
    df = df[df['NC'] != 'Nível Territorial (Código)'].copy()
    
    # Converter coluna de valor para numérico
    df['Rendimento'] = pd.to_numeric(df['V'], errors='coerce')
    
    # Renomear colunas para facilitar
    df.rename(columns={
        'D2N': 'Sexo',
        'D3N': 'Trimestre',
        'D4N': 'Cor_Raca'
    }, inplace=True)
    
    # Mostrar dados disponíveis
    print("📊 Dimensões disponíveis:")
    print(f"\n  Sexo: {df['Sexo'].unique()}")
    print(f"\n  Trimestres: {df['Trimestre'].unique()[:5]}...")  # Primeiros 5
    print(f"\n  Cor/Raça: {df['Cor_Raca'].unique()}")
    
    # Análise: Rendimento médio por sexo
    print("\n" + "="*60)
    print("💰 RENDIMENTO MÉDIO POR SEXO")
    print("="*60)
    
    rendimento_sexo = df.groupby('Sexo')['Rendimento'].mean()
    print(rendimento_sexo)
    
    # Calcular gap salarial
    if 'Homens' in rendimento_sexo.index and 'Mulheres' in rendimento_sexo.index:
        renda_homem = rendimento_sexo['Homens']
        renda_mulher = rendimento_sexo['Mulheres']
        gap = (1 - renda_mulher / renda_homem) * 100
        
        print(f"\n📉 GAP SALARIAL:")
        print(f"  Homens: R$ {renda_homem:,.2f}")
        print(f"  Mulheres: R$ {renda_mulher:,.2f}")
        print(f"  Diferença: R$ {renda_homem - renda_mulher:,.2f}")
        print(f"  Gap: {gap:.1f}% (mulheres ganham menos)")
    
    # Análise interseccional: Sexo + Cor/Raça
    print("\n" + "="*60)
    print("🔍 ANÁLISE INTERSECCIONAL (Sexo + Cor/Raça)")
    print("="*60)
    
    # Filtrar apenas dados válidos (não 'Total')
    df_filtrado = df[(df['Sexo'] != 'Total') & (df['Cor_Raca'] != 'Total')].copy()
    
    interseccional = df_filtrado.groupby(['Sexo', 'Cor_Raca'])['Rendimento'].mean()
    print(interseccional.sort_values(ascending=False))
    
    # Salvar dados
    df.to_csv('data/pnad_rendimento_completo.csv', index=False)
    print("\n💾 Dados salvos em: data/pnad_rendimento_completo.csv")
    
    # Salvar apenas dados de mulheres
    df_mulheres = df[df['Sexo'] == 'Mulheres'].copy()
    df_mulheres.to_csv('data/pnad_rendimento_mulheres.csv', index=False)
    print("💾 Dados de mulheres salvos em: data/pnad_rendimento_mulheres.csv")
    
    return df

if __name__ == "__main__":
    dados = buscar_rendimento_por_sexo()
    print("\n✅ Análise concluída!")
