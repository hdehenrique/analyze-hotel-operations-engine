# Desafio Data Scientist – Keycash

Este projeto foi desenvolvido para responder ao desafio proposto pela Keycash, simulando situações reais do dia a dia da operação de hotelaria e análise de dados.

## Objetivo

O objetivo do projeto é simular desafios reais de um Data Scientist na área de hotelaria, especialmente em cenários de expansão operacional, análise de reservas e saúde financeira do negócio.

## Funcionalidades Principais

- **Processamento de Dados:** Utiliza Apache Spark para ler, transformar e analisar grandes volumes de dados de reservas, clientes, operadoras e custos operacionais.
- **Forecast de Reservas:** Realiza previsões de volume de reservas e receita bruta para apoiar decisões de expansão.
- **Geração de Métricas e Relatórios:** Calcula métricas como taxa de ocupação, receita média, sazonalidade e margem operacional, além de identificar tendências e propor hipóteses para o negócio.
- **Estratégias Operacionais:** Analisa desafios de expansão, propõe soluções para otimização de processos, redução de custos e aumento de receita.

---

## Tecnologia Utilizada

- **Scala**: Linguagem principal para processamento e análise de dados.
- **Apache Spark**: Framework para processamento distribuído de grandes volumes de dados.
- **Maven**: Gerenciamento de dependências e build do projeto.
- **Java 8+**: Ambiente de execução necessário para o Spark e Scala.

---

## Premissas

- Os dados para resolução do desafio estão na planilha **Desafio_DS.xlsx**.
- Operação de hotelaria com locação de quartos por operadoras terceiras (Site A e Site B).
- Cada operadora cobra uma taxa de serviço.
- A empresa é responsável por toda a operação: registro, distribuição e limpeza dos quartos.
- Objetivo de expansão para **200 unidades** nos próximos dois meses (Dezembro/2019 e Janeiro/2020).

---

## Resumo da Solução

### 1. Estratégia para Expansão e Desafios Operacionais

- **Desafios Identificados:**
  - Gerenciamento de múltiplas unidades e equipes.
  - Controle de custos operacionais e taxas de serviço.
  - Manutenção da qualidade do serviço durante a expansão.
  - Otimização da ocupação e receita.

- **Soluções Propostas:**
  - Automatização de processos operacionais.
  - Monitoramento em tempo real de reservas e ocupação.
  - Parcerias estratégicas com operadoras.
  - Treinamento de equipes e padronização de processos.

### 2. Forecast de Reservas

- Utilizando os dados históricos de reservas diárias (Fevereiro a Novembro), foi realizado um forecast para os meses de Dezembro/2019 e Janeiro/2020.
- Técnicas de séries temporais (ex: ARIMA, Holt-Winters) foram aplicadas para estimar o volume de reservas e receita bruta esperada.

### 3. Métricas e Tendências Observadas

- **Métricas:** Taxa de ocupação, receita média por reserva, sazonalidade, taxa de cancelamento.
- **Tendências:** Identificação de períodos de alta e baixa demanda, influência de eventos sazonais, impacto das taxas das operadoras.
- **Hipóteses:** Crescimento em meses de férias, maior ocupação em finais de semana, influência de promoções das operadoras.

### 4. Saúde Financeira e Otimização de Lucros

- **Métricas Definidas:** Margem operacional, custo por unidade, receita líquida, taxa de conversão de reservas.
- **Avaliação:** Projeção de custos e receitas considerando a expansão para 200 unidades.
- **Estratégias de Otimização:**
  - Negociação de taxas com operadoras.
  - Redução de custos operacionais via automação.
  - Aumento da taxa de ocupação com campanhas direcionadas.

---

