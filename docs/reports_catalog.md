# BDI Report Catalog — Simplified Reference

Simplified reference for the BDI reports available through this project's
catalog (`src/b3_data_collector/bdi/_catalog.py`). For descriptions of what
each report contains, see [`report_descriptions_en.md`](report_descriptions_en.md)
(English) or [`report_descriptions_pt.md`](report_descriptions_pt.md) (Portuguese).

`api_name` is the identifier used internally by the catalog and the BDI
export API — this is what you'd reference when writing a new parser (see
`examples/read_single_bdi_report.py`). A dash (`—`) means the report has no
standard BDI download: it's either fetched through a separate endpoint
(tick-by-tick), or B3 doesn't currently publish a downloadable file for it.

## Renda Fixa (Fixed Income)

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Cadastro de instrumentos | OTC instrument list | `InstrumentRegistration` |
| Debêntures negociações compromissadas | Repo Debenture Transactions | `Repodebenture` |
| DI over | DI over | `DIover` |
| Estoque | OTC position | `Stock` |
| Liquidação | Settlement | `SaleOff` |
| Negociação consolidada | Consolidated negotiation | `ConsolidatedRecords` |
| Negociações compromissadas | Repurchase dealings | `RepurchaseDealings` |
| Negócio a negócio | Fixed income tick by tick | `Trade` |
| Negócios de VM de renda fixa realizados no Puma | Fixed income securities trades conducted on Puma | `DebenturesBusiness` |
| Registro | Register | `Register` |
| TDA - Valores nominais | TDA - Nominal values | — *(no download available)* |

## Renda Variável (Equities) — Resumo de ações

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Ações | Equities | `DailyAverageStocks` |
| Ações - resumo das operações | Equities - transactions summary | `StocksOperationSummary` |
| Cadastro de instrumentos | Instruments consolidated | `InstrumentsEquities` |
| Cenários de margem para ativos líquidos | Margin scenarios for liquid assets | `MarginScenarios` |
| Comportamento dos valores de referência das cotas dos ETFs (IOPV) | Indicative optimized portfolio value (IOPV) ETFs | `IOPV` |
| Mercado a termo | Forward market | `ForwardMarket` |
| Médias diárias – volume em um ano (R$ em milhões) | Daily averages - annual volume (BRL million) | `AverageChart` |
| Negociação de estratégias | Trading strategies | `NegotiStrategi` |
| Negócios consolidados do pregão | Consolidated trades of the session | `ConsolidatedTradesEquities` |
| Negócios consolidados do pregão não regular | Consolidated trades of the non-regular session | `ConsolidatedTradesRVAfter` |
| Negócio a negócio (tick by tick) | Tick by tick | — *(fetched via `tick_by_tick` pipeline, not the BDI API)* |

## Renda Variável — Indicadores e informativos

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Composição das carteiras dos índices | Composition of indices' portfolios | `PreviaQuadrimestral` |
| Distribuição dos locais de formação de lotes café Conilon | Conilon coffee price formation centers | `CNLDistrionCoffeLocation` |
| Distribuição dos locais de formação de lotes café tipo 4/5 | Type 4/5 coffee price formation centers | `DistrionCoffeLocation` |
| Estatísticas de safras - café Arábica 4/5 | Crop statistics - approved - arabica coffee 4/5 | `HarvestCoffeApprov` |
| Estatísticas de safras - café Conilon | Crop statistics - approved - Conilon coffee | `CNLHarvestCoffeApprov` |
| Evolução dos índices | Indexes evolution | `INDEXES` |
| Histórico de taxas de câmbio (Resolução BCB nº 120) | Exchange rates history (Resolution BCB no. 120) | `HistoricalExchange` |
| Indicadores econômicos | Economic indicators | `EconomicIndicators` |
| Informativos | Updates | — *(no download available)* |
| Lotes válidos para liquidação - café Arábica | Valid lots for contract settlement - arabica coffee | `ValidLotsSettle` |
| Lotes válidos para liquidação - café Conilon | Valid lots for contract settlement - Conilon coffee | `CNLValidLotsSettle` |
| Participação dos investidores | Participation of investors | `SharesInvesVolum` |
| Participação dos investidores mensal | Participation of investors monthly | `SharesInvesVolumMonthly` |
| Prévia das carteiras teóricas de índices | Preview of indices' theoretical portfolios | — *(report not published by B3)* |

## Renda Variável — Maiores oscilações

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Ações do IBOVESPA - maiores altas | IBOVESPA equities - largest gains | `IbovespaStockBiggestHighs` |
| Ações do IBOVESPA - maiores baixas | IBOVESPA equities - largest losses | `IbovespaStockBiggestLow` |
| Ações mais negociadas - a termo | Most traded equities - forward | `Forward` |
| Opções mais negociadas - opções de compra | Most traded equities - call options | `OptionsPurshase` |
| Opções mais negociadas - opções de venda | Most traded equities - put options | `OptionsSelling` |
| Ações mais negociadas - à vista | Most traded equities - cash | `InCash` |
| Mercado à vista - maiores altas | Cash market - largest gains | `InCashMarketBiggestHighs` |
| Mercado à vista - maiores baixas | Cash market - largest losses | `InCashMarketBiggestLow` |

## Derivativos — Resumo

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Derivativos | Derivatives | `DailyAverageDerivatives2` |
| Derivativos - mark to market | Derivatives - mark to market | `DerivativesMtM` |
| Derivativos - resumo das operações | Derivatives - transactions summary | `DerivativesOperation2` |

## Derivativos — Derivativos de balcão (OTC)

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Estoque com CCP | Inventory with CCP | `OTCInventoryCCP` |
| Estoque sem CCP | Inventory without CCP | `OTCInventoryWCCP` |
| Opções flexíveis | Flexible options | `FlexibleOptions` |
| Registro com CCP | Record with CCP | `OTCRegistrationCCP` |
| Registro sem CCP | Record without CCP | `OTCRegistrationWCCP` |
| Swap | Swap | `SwapFlex` |
| Termo eletrônico | Electronic term | — *(no download available)* |

## Derivativos — Derivativos de bolsa (Exchange-traded)

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Cadastro de instrumentos | Instruments consolidated | `InstrumentsDerivatives` |
| Negócio a negócio (tick by tick) | Tick by tick | — *(fetched via `tick_by_tick` pipeline, not the BDI API)* |
| Negócios consolidados do pregão | Consolidated trades of the session | `ConsolidatedTradesDerivatives` |
| Negócios consolidados do pregão não regular | Consolidated trades of the non-regular session | `ConsolidatedTradesDerivativesAfter` |
| Posições em aberto | Open positions | `OpenPositionsEquities` |

## Outros Dados — Clearing e depositária

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Ações custodiadas - programa de ADR | Equities in custody - ADR program | `Custody` |
| Crédito de proventos | Earnings credited | `ProventionCreditVariable` |
| Custódia fungível | Fungible custody | `FugibleCustody` |
| Prazo para depósito de títulos | Period to deposit securities | `DeadlineDepositSecurities` |

## Outros Dados — Empréstimo de ativos (Securities Lending)

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Empréstimos registrados | Registered securities lending | `BTBLoanBalance` |
| Negócios | Trades | `BTBTrade` |
| Posições em aberto | Open positions | `BTBLendingOpenPosition` |
| Renovações | Renewals | `Renewals` |

## Outros Dados — COE

| Report (PT) | Report (EN) | API Name |
|---|---|---|
| Estoque | Inventory | `COEInventory` |
| Registro | Records | `COERegistration` |