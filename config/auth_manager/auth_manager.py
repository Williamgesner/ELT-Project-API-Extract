# Gerenciador de Autenticação: responsável por gerar um novo Access_token sempre que expirar!

"""
=============================================================================
GERENCIADOR DE AUTENTICAÇÃO AUTOMÁTICA DA API BLING
=============================================================================

Responsável por:
- Renovar access_token automaticamente usando refresh_token
- Atualizar o arquivo .env com os novos tokens
- Verificar expiração e emitir avisos
- Garantir que os pipelines sempre tenham tokens válidos

ORGANIZAÇÃO:
- Este arquivo fica em: config/auth_manager/auth_manager.py
- Arquivos de controle em: config/auth_manager/.token_control_XX.txt
- Arquivo .env na raiz do projeto

=============================================================================
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv, set_key
import time

# =====================================================
# CONFIGURAÇÃO DE CAMINHOS
# =====================================================

# Diretório onde ESTE arquivo está (config/auth_manager/)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Diretório raiz do projeto (2 níveis acima: Projeto/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))

# Caminho completo do arquivo .env (na raiz do projeto)
ENV_FILE_PATH = os.path.join(PROJECT_ROOT, '.env')

# Carregar variáveis do .env
load_dotenv(ENV_FILE_PATH)


class BlingAuthManager:
    """
    Gerenciador de autenticação que renova tokens automaticamente
    
    Atributos:
        empresa_id (int): ID da empresa (1 a 6)
        client_id (str): Client ID da aplicação Bling
        client_secret (str): Client Secret da aplicação Bling
        refresh_token (str): Refresh token para renovação automática
    """
    
    def __init__(self, empresa_id):
        """
        Inicializa o gerenciador para uma empresa específica
        
        Args:
            empresa_id (int): ID da empresa (1 a 6)
            
        Raises:
            Exception: Se as credenciais não forem encontradas no .env
        """
        self.empresa_id = empresa_id
        
        # ✅ USAR A VARIÁVEL GLOBAL DEFINIDA NO TOPO
        self.env_file = ENV_FILE_PATH
        
        # URLs da API Bling para autenticação
        self.token_url = "https://bling.com.br/Api/v3/oauth/token"
        
        # Carregar credenciais do .env
        self.client_id = os.getenv(f"CLIENT_ID_{empresa_id:02d}")
        self.client_secret = os.getenv(f"CLIENT_SECRET_{empresa_id:02d}")
        self.refresh_token = os.getenv(f"REFRESH_TOKEN_{empresa_id:02d}")
        
        # Validar se todas as credenciais existem
        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise Exception(
                f"❌ Credenciais incompletas para Empresa {empresa_id:02d}!\n"
                f"   Verifique se CLIENT_ID_{empresa_id:02d}, CLIENT_SECRET_{empresa_id:02d} "
                f"e REFRESH_TOKEN_{empresa_id:02d} estão no .env"
            )
        
        print(f"✅ Auth Manager inicializado para Empresa {empresa_id:02d}")
    
    
    def renovar_access_token(self):
        """
        Renova o access_token usando o refresh_token
        
        O Bling retorna:
        - Novo access_token (válido por 6 horas)
        - Novo refresh_token (válido por mais 30 dias) ← IMPORTANTE!
        
        Returns:
            str: Novo access_token ou None se falhar
        """
        print(f"\n🔄 Renovando access_token para Empresa {self.empresa_id:02d}...")
        
        try:
            # Dados para enviar na requisição
            payload = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token
            }
            
            # Autenticação Basic (client_id:client_secret em Base64)
            from requests.auth import HTTPBasicAuth
            auth = HTTPBasicAuth(self.client_id, self.client_secret)
            
            # Fazer a requisição para o Bling
            print(f"   📡 Enviando requisição para {self.token_url}")
            response = requests.post(
                self.token_url,
                data=payload,
                auth=auth,
                timeout=30
            )
            
            # Verificar se deu certo
            if response.status_code == 200:
                dados = response.json()
                
                novo_access_token = dados.get('access_token')
                novo_refresh_token = dados.get('refresh_token')  # Bling envia um novo!
                expires_in = dados.get('expires_in', 21600)  # Padrão 6h
                
                print(f"✅ Token renovado com sucesso!")
                print(f"   • Novo access_token: {novo_access_token[:30]}...")
                print(f"   • Validade: {expires_in / 3600:.1f} horas")
                
                if novo_refresh_token:
                    print(f"   • Novo refresh_token: {novo_refresh_token[:30]}... ✨")
                    print(f"   • ⚠️  IMPORTANTE: Refresh token também foi renovado (+30 dias)!")
                
                # Atualizar o .env com os novos tokens
                self._atualizar_env(novo_access_token, novo_refresh_token)
                
                return novo_access_token
                
            else:
                print(f"❌ Erro ao renovar token: HTTP {response.status_code}")
                print(f"   Resposta: {response.text}")
                
                # Mensagens de erro específicas
                if response.status_code == 401:
                    print(f"   ⚠️  POSSÍVEL CAUSA: Refresh token expirado ou inválido")
                    print(f"   📋 SOLUÇÃO: Gerar novo refresh_token no Postman")
                elif response.status_code == 400:
                    print(f"   ⚠️  POSSÍVEL CAUSA: Client ID/Secret incorretos")
                
                return None
                
        except requests.exceptions.ConnectionError:
            print(f"❌ Erro de conexão: Verifique sua internet")
            return None
        except requests.exceptions.Timeout:
            print(f"❌ Timeout: API do Bling não respondeu em 30s")
            return None
        except Exception as e:
            print(f"❌ Erro inesperado ao renovar token: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    
    def _atualizar_env(self, novo_access_token, novo_refresh_token=None):
        """
        Atualiza o .env com os novos tokens
        
        CRÍTICO: Esta função garante que a "corrente de renovação" nunca quebra,
        atualizando tanto o access_token quanto o refresh_token no .env
        
        Args:
            novo_access_token (str): Novo access token
            novo_refresh_token (str, optional): Novo refresh token (se o Bling enviar)
        """
        try:
            print(f"\n   💾 Atualizando arquivo .env...")
            print(f"   📂 Caminho: {self.env_file}")
            
            # Atualizar API_KEY (access_token)
            set_key(self.env_file, f"API_KEY_{self.empresa_id:02d}", novo_access_token)
            print(f"   ✅ API_KEY_{self.empresa_id:02d} atualizada")
            
            # Se o Bling enviou um novo refresh_token, atualizar também
            # ISSO É CRÍTICO! Garante que o refresh_token nunca expire
            if novo_refresh_token:
                set_key(self.env_file, f"REFRESH_TOKEN_{self.empresa_id:02d}", novo_refresh_token)
                self.refresh_token = novo_refresh_token  # Atualizar em memória também
                print(f"   ✅ REFRESH_TOKEN_{self.empresa_id:02d} atualizada ✨")
                print(f"   📅 Novo prazo de validade: +30 dias a partir de agora")
            
            # Recarregar as variáveis de ambiente
            load_dotenv(self.env_file, override=True)
            
            print(f"   ✅ Arquivo .env sincronizado com sucesso!")
            
        except Exception as e:
            print(f"   ⚠️  Erro ao atualizar .env: {e}")
            print(f"   ⚠️  O token foi renovado mas NÃO foi salvo no .env!")
            print(f"   ⚠️  Atualize manualmente: API_KEY_{self.empresa_id:02d}")
    
    
    def obter_token_valido(self):
        """
        Obtém um access_token válido (renova automaticamente se necessário)
        
        Esta é a função PRINCIPAL que vai chamar nos pipelines
        
        ESTRATÉGIA:
        - Sempre renova preventivamente (mais seguro)
        - Como o Bling renova o refresh_token junto, não há problema em renovar sempre
        - Garante token sempre fresco, evitando erros 401
        
        Returns:
            str: Access token válido
        """
        print(f"\n{'='*60}")
        print(f"🔑 OBTENDO TOKEN VÁLIDO - EMPRESA {self.empresa_id:02d}")
        print(f"{'='*60}")
        
        # Buscar o token atual do .env
        token_atual = os.getenv(f"API_KEY_{self.empresa_id:02d}")
        
        if not token_atual:
            print(f"⚠️  Nenhum token encontrado no .env, gerando novo...")
            return self.renovar_access_token()
        
        # ESTRATÉGIA: Renovar preventivamente a cada execução
        # Isso garante que sempre teremos um token novo
        # E mantém a "corrente de renovação" do refresh_token ativa
        
        print(f"🔄 Renovando token preventivamente (estratégia de segurança)...")
        novo_token = self.renovar_access_token()
        
        if novo_token:
            print(f"\n{'='*60}")
            print(f"✅ TOKEN VÁLIDO OBTIDO COM SUCESSO!")
            print(f"{'='*60}")
            return novo_token
        else:
            print(f"\n⚠️  Falha ao renovar, usando token atual do .env como fallback")
            print(f"   Token atual: {token_atual[:30]}...")
            print(f"\n{'='*60}")
            print(f"⚠️  USANDO TOKEN ATUAL (pode estar expirado)")
            print(f"{'='*60}")
            return token_atual
    
    
    def verificar_expiracao_refresh_token(self):
        """
        Verifica se o refresh_token está próximo de expirar
        
        NOTA IMPORTANTE:
        Como o Bling renova o refresh_token a cada renovação do access_token,
        isso só será um problema se o sistema ficar parado por mais de 30 dias.
        
        Se rodar o pipeline pelo menos 1x a cada 30 dias, o refresh_token
        NUNCA expira! 
        
        Arquivos de controle salvos em: config/auth_manager/.token_control_XX.txt
        """
        print(f"\n📅 Verificando validade do refresh_token...")
        
        # ✅ USAR O DIRETÓRIO CORRETO (config/auth_manager/)
        controle_file = os.path.join(CURRENT_DIR, f'.token_control_{self.empresa_id:02d}.txt')
        
        try:
            # Tentar ler a última renovação
            if os.path.exists(controle_file):
                with open(controle_file, 'r') as f:
                    ultima_renovacao = datetime.fromisoformat(f.read().strip())
                
                # Calcular há quanto tempo foi
                dias_desde_renovacao = (datetime.now() - ultima_renovacao).days
                dias_restantes = 30 - dias_desde_renovacao
                
                print(f"   📊 Última renovação: há {dias_desde_renovacao} dia(s)")
                print(f"   ⏰ Dias restantes (se sistema ficar parado): {dias_restantes}")
                print(f"   📂 Arquivo de controle: {os.path.basename(controle_file)}")
                
                # Avisos progressivos
                if dias_restantes <= 3:
                    print(f"\n   {'🔴'*20}")
                    print(f"   🔴 CRÍTICO! Menos de 3 dias!")
                    print(f"   🔴 Se o sistema NÃO rodar nos próximos {dias_restantes} dias,")
                    print(f"   🔴 o refresh_token vai EXPIRAR!")
                    print(f"   🔴 Execute o pipeline OU renove manualmente no Postman")
                    print(f"   {'🔴'*20}\n")
                elif dias_restantes <= 7:
                    print(f"   ⚠️  ATENÇÃO! Menos de 7 dias se sistema ficar parado!")
                    print(f"   💡 Execute o pipeline esta semana para renovar automaticamente")
                elif dias_restantes <= 15:
                    print(f"   ℹ️  Refresh token expira em {dias_restantes} dias (se sistema parar)")
                else:
                    print(f"   ✅ Refresh token válido ({dias_restantes} dias restantes)")
                    print(f"   💡 Lembre-se: roda o pipeline 1x/mês e nunca expira!")
            else:
                print(f"   ℹ️  Primeira execução, criando controle...")
                print(f"   📂 Local: config/auth_manager/.token_control_{self.empresa_id:02d}.txt")
                
            # Atualizar data da última renovação
            with open(controle_file, 'w') as f:
                f.write(datetime.now().isoformat())
            
            print(f"   ✅ Controle de expiração atualizado")
                
        except Exception as e:
            print(f"   ⚠️  Erro ao verificar expiração: {e}")
            print(f"   ℹ️  Isso não impede o funcionamento do sistema")


# =============================================================================
# FUNÇÃO HELPER (FACILITA O USO)
# =============================================================================

def obter_token_para_empresa(empresa_id):
    """
    Função helper para obter token válido de forma simples
    
    Esta é a função que vai usar nos pipelines!
    
    Args:
        empresa_id (int): ID da empresa (1 a 6)
    
    Returns:
        str: Access token válido
    
    Exemplo de uso:
        >>> from config.auth_manager import obter_token_para_empresa
        >>> 
        >>> # Obter token da Empresa 01
        >>> token = obter_token_para_empresa(1)
        >>> 
        >>> # Usar o token nas requisições
        >>> headers = {
        >>>     "Accept": "application/json",
        >>>     "Authorization": f"Bearer {token}"
        >>> }
    """
    manager = BlingAuthManager(empresa_id)
    manager.verificar_expiracao_refresh_token()  # Verifica e avisa
    return manager.obter_token_valido()


# =============================================================================
# BLOCO PARA TESTE
# =============================================================================

if __name__ == "__main__":
    """
    Bloco de teste - roda quando executa: 
    python3 -m analysis.teste_auth   
    
    Testa a renovação do token para a Empresa 01
    """
    
    print("\n")
    print("="*60)
    print("🧪 TESTANDO AUTH MANAGER - RENOVAÇÃO AUTOMÁTICA")
    print("="*60)
    print(f"📂 Arquivo: {__file__}")
    print(f"📂 Diretório atual: {CURRENT_DIR}")
    print(f"📂 Raiz do projeto: {PROJECT_ROOT}")
    print(f"📂 Arquivo .env: {ENV_FILE_PATH}")
    print("="*60)
    print("\n")
    
    # Testar para a Empresa 01
    try:
        print("📋 Iniciando teste para Empresa 01...")
        print("-"*60)
        
        # Obter token (vai renovar automaticamente)
        token = obter_token_para_empresa(1)
        
        print("\n")
        print("-"*60)
        print("✅ RESULTADO DO TESTE:")
        print("-"*60)
        print(f"✅ Token obtido com sucesso!")
        print(f"✅ Primeiros 40 caracteres: {token[:40]}...")
        print(f"✅ Tamanho total: {len(token)} caracteres")
        print(f"✅ Tipo: {type(token)}")
        
        # Verificar se foi salvo no .env
        load_dotenv(ENV_FILE_PATH, override=True)
        token_no_env = os.getenv("API_KEY_01")
        
        if token_no_env == token:
            print(f"✅ Token salvo corretamente no .env")
        else:
            print(f"⚠️  Token no .env difere do retornado (pode ser normal)")
        
        # Verificar se arquivo de controle foi criado
        controle_file = os.path.join(CURRENT_DIR, '.token_control_01.txt')
        if os.path.exists(controle_file):
            print(f"✅ Arquivo de controle criado: {os.path.basename(controle_file)}")
        
        print("\n")
        print("="*60)
        print("🎉 TESTE CONCLUÍDO COM SUCESSO!")
        print("="*60)
        print("\n")
        print("💡 PRÓXIMOS PASSOS:")
        print("   1. Verifique se o arquivo .env foi atualizado")
        print("   2. Verifique se API_KEY_01 tem um novo valor")
        print("   3. Verifique se existe .token_control_01.txt em config/auth_manager/")
        print("   4. Se sim, está tudo funcionando perfeitamente!")
        print("\n")
        
    except Exception as e:
        print("\n")
        print("="*60)
        print("❌ ERRO NO TESTE")
        print("="*60)
        print(f"\n❌ {e}\n")
        
        import traceback
        print("📋 Detalhes do erro:")
        print("-"*60)
        traceback.print_exc()
        print("-"*60)
        
        print("\n💡 POSSÍVEIS CAUSAS:")
        print("   • Credenciais não configuradas no .env")
        print("   • Client ID ou Client Secret incorretos")
        print("   • Refresh token expirado (gerar novo no Postman)")
        print("   • Problemas de conexão com a API do Bling")
        print("   • Caminhos de arquivo incorretos")
        print(f"\n📂 VERIFICAR:")
        print(f"   • .env existe em: {ENV_FILE_PATH}?")
        print(f"   • Diretório auth_manager: {CURRENT_DIR}")
        print("\n")