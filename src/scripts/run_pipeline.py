#!/usr/bin/env python3
"""
Script para executar a pipeline de processamento de dados de livros.

Este script fornece uma interface de linha de comando para executar
a pipeline completa ou partes específicas dela.

Uso:
    python run_pipeline.py                    # Executa pipeline completa
    python run_pipeline.py --cleaning-only   # Apenas limpeza
    python run_pipeline.py --features-only   # Apenas features
    python run_pipeline.py --config custom_config.json  # Config customizada
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

# Adicionar o diretório scripts ao path para imports
sys.path.append(str(Path(__file__).parent))

from data_types import PipelineConfig
from main_pipeline import run_full_pipeline, run_cleaning_only, run_features_only


def load_config_from_file(config_path: str) -> PipelineConfig:
    """
    Carrega configuração de um arquivo JSON.
    
    Args:
        config_path: Caminho para o arquivo de configuração
        
    Returns:
        PipelineConfig: Configuração carregada
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        return PipelineConfig(**config_data)
        
    except Exception as e:
        print(f"Erro ao carregar configuração de {config_path}: {str(e)}")
        sys.exit(1)


def create_default_config_file(output_path: str) -> None:
    """
    Cria um arquivo de configuração padrão.
    
    Args:
        output_path: Caminho onde salvar a configuração
    """
    config = PipelineConfig()
    config_dict = config.model_dump()
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config_dict, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Configuração padrão criada em: {output_path}")
        print("Edite este arquivo para customizar a pipeline.")
        
    except Exception as e:
        print(f"Erro ao criar configuração padrão: {str(e)}")
        sys.exit(1)


def print_pipeline_info():
    """Imprime informações sobre a pipeline."""
    print("=" * 60)
    print("PIPELINE DE PROCESSAMENTO DE DADOS DE LIVROS")
    print("=" * 60)
    print()
    print("Esta pipeline executa as seguintes etapas:")
    print()
    print("🧹 LIMPEZA DE DADOS:")
    print("  • Verificação de valores nulos")
    print("  • Criação de IDs únicos")
    print("  • Tratamento de categorias problemáticas")
    print("  • Transformação da coluna availability (yes/no → 1/0)")
    print("  • Salvamento em data/processed/")
    print()
    print("⚙️ FEATURE ENGINEERING:")
    print("  • price_range (categorização de preços)")
    print("  • Features de título (subtitle, series, starts_with_the)")
    print("  • title_length e word_count")
    print("  • Categorização de ratings e stock")
    print("  • One-hot encoding para categorias")
    print("  • Score de popularidade")
    print("  • Salvamento em data/features/")
    print()
    print("=" * 60)


def main():
    """Função principal do script."""
    parser = argparse.ArgumentParser(
        description="Pipeline de processamento de dados de livros",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:

  # Executar pipeline completa com configuração padrão
  python run_pipeline.py

  # Executar apenas limpeza
  python run_pipeline.py --cleaning-only

  # Executar apenas features (requer dados processados)
  python run_pipeline.py --features-only

  # Usar configuração customizada
  python run_pipeline.py --config my_config.json

  # Criar arquivo de configuração padrão
  python run_pipeline.py --create-config config.json

  # Mostrar informações sobre a pipeline
  python run_pipeline.py --info
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Caminho para arquivo de configuração JSON'
    )
    
    parser.add_argument(
        '--cleaning-only',
        action='store_true',
        help='Executar apenas a pipeline de limpeza'
    )
    
    parser.add_argument(
        '--features-only',
        action='store_true',
        help='Executar apenas a pipeline de feature engineering'
    )
    
    parser.add_argument(
        '--create-config',
        type=str,
        metavar='PATH',
        help='Criar arquivo de configuração padrão no caminho especificado'
    )
    
    parser.add_argument(
        '--info',
        action='store_true',
        help='Mostrar informações sobre a pipeline'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Executar em modo verboso (mais logs)'
    )
    
    args = parser.parse_args()
    
    # Mostrar informações
    if args.info:
        print_pipeline_info()
        return
    
    # Criar configuração padrão
    if args.create_config:
        create_default_config_file(args.create_config)
        return
    
    # Configurar logging verboso se solicitado
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Carregar configuração
    config = None
    if args.config:
        config = load_config_from_file(args.config)
        print(f"Usando configuração de: {args.config}")
    else:
        config = PipelineConfig()
        print("Usando configuração padrão")
    
    try:
        # Executar pipeline baseado nos argumentos
        if args.cleaning_only and args.features_only:
            print("❌ Erro: --cleaning-only e --features-only são mutuamente exclusivos")
            sys.exit(1)
        
        elif args.cleaning_only:
            print("🧹 Executando apenas pipeline de limpeza...")
            stats = run_cleaning_only(config)
            print(f"✅ Limpeza concluída! {stats['processed_records']} registros processados")
        
        elif args.features_only:
            print("⚙️ Executando apenas pipeline de features...")
            stats = run_features_only(config)
            print(f"✅ Features concluídas! {stats['features_created']} features criadas")
        
        else:
            print("🚀 Executando pipeline completa...")
            stats = run_full_pipeline(config)
            print(f"✅ Pipeline completa! Executada em {stats.execution_time_seconds:.2f}s")
            
            # Mostrar resumo final
            print(f"📊 Registros processados: {stats.total_records}")
            print(f"⚙️ Features criadas: {stats.features_created}")
            print(f"📁 Dados processados: {config.processed_output}")
            print(f"🎯 Dados com features: {config.features_output}")
    
    except KeyboardInterrupt:
        print("\n⚠️ Pipeline interrompida pelo usuário")
        sys.exit(130)
    
    except Exception as e:
        print(f"❌ Erro na execução da pipeline: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()