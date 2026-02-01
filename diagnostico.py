import requests
import urllib3
import json

# Desabilita o aviso de SSL para focar no erro real
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("=== INICIANDO DIAGNÓSTICO DE CONEXÃO SICONFI ===\n")

url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rgf"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json"
}

# Cenário 1: Parâmetros originais (que deram erro 400)
params_1 = {
    'an_exercicio': 2024, # Testando um ano garantido
    'nr_periodo': 2,
    'in_periodicidade': 'Q',
    'co_poder': 'E',
    'co_esfera': 'E',
    'id_ente': 53 # DF
}

# Cenário 2: Adicionando o Tipo de Demonstrativo (Provável Solução)
params_2 = params_1.copy()
params_2['co_tipo_demonstrativo'] = 'RGF'

# Cenário 3: Adicionando o Anexo Específico
params_3 = params_2.copy()
params_3['no_anexo'] = 'RGF-Anexo 01' # Nome técnico comum

def testar(nome_teste, params):
    print(f"🔄 Testando: {nome_teste}...")
    try:
        response = requests.get(url, params=params, headers=headers, verify=False, timeout=10)
        print(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            dados = response.json()['items']
            print(f"   ✅ SUCESSO! Itens retornados: {len(dados)}")
            if len(dados) > 0:
                print(f"   Exemplo de conta: {dados[0]['conta']}")
        else:
            print(f"   ❌ ERRO. Mensagem do Servidor:")
            # Tenta mostrar o erro formatado, ou o texto bruto
            try:
                print(f"   {json.dumps(response.json(), indent=2)}") 
            except:
                print(f"   {response.text[:300]}") # Mostra os primeiros 300 caracteres do erro
    except Exception as e:
        print(f"   ⚠️ Falha crítica de conexão: {e}")
    print("-" * 50)

# Rodar os testes
testar("Cenário 1 (Parâmetros Atuais)", params_1)
testar("Cenário 2 (Com co_tipo_demonstrativo='RGF')", params_2)
testar("Cenário 3 (Com Anexo Específico)", params_3)