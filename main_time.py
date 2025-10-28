# Responsável por: Popular a tabela dim_tempo com todas as datas necessárias
# ⚠️ Esse deve ser rodado antes de main_transform_sales e depois de main_transform_contacts !  
# ⚠️ Só roda esse Script uma vez, ou quando atingir a data limite adicionada na linha 158 !

import pandas as pd
from datetime import datetime
from config.database import create_schema_processed, create_all_tables, engine
from sqlalchemy import text

# =====================================================
# 1. FUNÇÃO PARA POPULAR DIM_TEMPO
# =====================================================

def popular_dim_tempo(data_inicio='2015-01-01', data_fim='2035-12-31'):
    """
    Popula a dimensão tempo com todas as datas no intervalo
    
    Args:
        data_inicio: Data inicial (padrão: 2020-01-01)
        data_fim: Data final (padrão: 2035-12-31)
    """
    print("\n" + "=" * 70)
    print("📅 POPULANDO DIM_TEMPO")
    print("=" * 70)
    print(f"Período: {data_inicio} até {data_fim}")
    print("=" * 70)
    
    inicio = datetime.now()
    
    # Gerar todas as datas
    print("\n1️⃣ Gerando datas...")
    datas = pd.date_range(start=data_inicio, end=data_fim, freq='D')
    print(f"✅ {len(datas)} datas geradas")
    
    # Criar DataFrame com atributos
    print("\n2️⃣ Criando atributos das datas...")
    registros = []
    
    nomes_meses = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }
    
    nomes_meses_abrev = {
        1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr',
        5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Ago',
        9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }
    
    nomes_dias_semana = {
        0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira',
        3: 'Quinta-feira', 4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'
    }
    
    nomes_dias_semana_abrev = {
        0: 'Seg', 1: 'Ter', 2: 'Qua',
        3: 'Qui', 4: 'Sex', 5: 'Sab', 6: 'Dom'
    }
    
    for data in datas:
        registro = {
            'data_completa': data.date(),
            'ano': data.year,
            'mes': data.month,
            'dia': data.day,
            'trimestre': (data.month - 1) // 3 + 1,
            'semestre': 1 if data.month <= 6 else 2,
            'nome_mes': nomes_meses[data.month],
            'nome_mes_abrev': nomes_meses_abrev[data.month],
            'dia_semana': data.weekday(),
            'nome_dia_semana': nomes_dias_semana[data.weekday()],
            'nome_dia_semana_abrev': nomes_dias_semana_abrev[data.weekday()],
            'eh_fim_semana': data.weekday() >= 5,  # Sábado=5, Domingo=6
            'eh_feriado': False,  # Pode ser customizado depois
            'semana_ano': data.isocalendar()[1]
        }
        registros.append(registro)
    
    df = pd.DataFrame(registros)
    print(f"✅ Atributos criados para {len(df)} datas")
    
    # Inserir no banco
    print("\n3️⃣ Inserindo no banco de dados...")
    
    try:
        df.to_sql(
            name='dim_tempo',
            con=engine,
            schema='processed',
            if_exists='append',  # Adiciona novos registros
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"✅ {len(registros)} datas inseridas com sucesso!")
        
        # Verificar
        with engine.connect() as conn:
            query = text("SELECT COUNT(*) FROM processed.dim_tempo")
            total = conn.execute(query).scalar()
            print(f"✅ Verificação: {total} registros na tabela")
        
    except Exception as e:
        if "duplicate key" in str(e).lower():
            print(f"⚠️  Algumas datas já existiam no banco (ignoradas)")
            print(f"💡 Use TRUNCATE TABLE processed.dim_tempo; para recriar")
        else:
            print(f"❌ Erro ao inserir: {e}")
            raise
    
    fim = datetime.now()
    tempo_total = fim - inicio
    
    print(f"\n{'='*70}")
    print(f"🎉 DIM_TEMPO POPULADA COM SUCESSO!")
    print(f"⏱️  Tempo total: {tempo_total}")
    print(f"{'='*70}")
    
    # Exemplos de registros
    print(f"\n📋 EXEMPLOS DE REGISTROS:")
    with engine.connect() as conn:
        query = text("""
            SELECT 
                data_completa,
                nome_dia_semana_abrev,
                dia,
                nome_mes_abrev,
                ano,
                trimestre
            FROM processed.dim_tempo
            ORDER BY data_completa
            LIMIT 5
        """)
        
        resultado = conn.execute(query)
        print("\n   data       | dia_sem | dia | mês | ano  | tri")
        print("   " + "-" * 50)
        for row in resultado:
            print(f"   {row.data_completa} | {row.nome_dia_semana_abrev:7} | {row.dia:2}  | {row.nome_mes_abrev:3} | {row.ano} | Q{row.trimestre}")

# =====================================================
# 2. EXECUÇÃO DO SCRIPT
# =====================================================

if __name__ == "__main__":
    try:
        # Criar schema processed se não existir
        create_schema_processed()
        
        # Criar tabelas (incluindo dim_tempo)
        create_all_tables()
        
        # Popular dim_tempo
        popular_dim_tempo(
            data_inicio='2015-01-01',  # Ajustar conforme necessário
            data_fim='2035-12-31'      # Ajustar conforme necessário
        )
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Verificar dados: SELECT * FROM processed.dim_tempo LIMIT 10;")
        print(f"   2. Executar transformação de vendas: python main_transform_vendas.py")
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        raise