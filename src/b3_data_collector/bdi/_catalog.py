# src\b3_data_collector\bdi\_catalog.py

"""
BDI report catalog.

Single source of truth for every BDI report available for download.
Each ``ReportDefinition`` entry carries the information the pipeline
needs to download and store the report:

- ``api_name``  : the ``Name`` field sent in the POST body to the BDI
                  export endpoint — must match exactly.
- ``section``   : top-level section on the B3 website (used as the first
                  path component in the S3 key).
- ``name_pt``   : Portuguese display name, for logging and reference.
- ``name_en``   : English display name, for logging and reference.
- ``sla``       : time by which B3 guarantees the report is available
                  (HH:MM:SS, Brazil time). Useful for scheduling.
- ``enabled``   : when ``False``, the pipeline skips this report without
                  error. Flip to ``False`` for reports you never need
                  rather than deleting the entry.

Reports confirmed as having no download (TDA, Termo eletrônico,
Informativos, Prévia das carteiras) are intentionally absent.

Source: HAR capture of arquivos.b3.com.br — June 2026.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class ReportDefinition:
    """
    Immutable descriptor for a single BDI downloadable report.

    Parameters
    ----------
    api_name : str
        Value sent as ``Name`` in the POST body to the export endpoint.
    section : str
        Top-level grouping used as the first S3 path component.
    name_pt : str
        Portuguese display name (from the B3 website).
    name_en : str
        English display name (from the B3 website).
    sla : str
        Latest time B3 guarantees availability (``"HH:MM:SS"``, BRT).
    enabled : bool
        Whether the pipeline should download this report. Default ``True``.
    """

    api_name : str
    section  : str
    name_pt  : str
    name_en  : str
    sla      : str
    enabled  : bool = True


# ---------------------------------------------------------------------------
# Full catalog — 63 downloadable reports
# ---------------------------------------------------------------------------

CATALOG: Final[tuple[ReportDefinition, ...]] = (

    # ── Renda fixa ──────────────────────────────────────────────────────────
    ReportDefinition("InstrumentRegistration", "renda_fixa",
        "Cadastro de instrumentos",                       "OTC instrument list",                              "08:00:00"),
    ReportDefinition("Repodebenture",          "renda_fixa",
        "Debêntures negociações compromissadas",          "Repo Debenture Transactions",                      "08:00:00"),
    ReportDefinition("DIover",                 "renda_fixa",
        "DI over",                                        "DI over",                                          "08:00:00"),
    ReportDefinition("Stock",                  "renda_fixa",
        "Estoque",                                        "OTC position",                                     "08:00:00"),
    ReportDefinition("SaleOff",                "renda_fixa",
        "Liquidação",                                     "Settlement",                                       "08:00:00"),
    ReportDefinition("ConsolidatedRecords",    "renda_fixa",
        "Negociação consolidada",                         "Consolidated negotiation",                         "08:00:00"),
    ReportDefinition("RepurchaseDealings",     "renda_fixa",
        "Negociações compromissadas",                     "Repurchase dealings",                              "08:00:00"),
    ReportDefinition("Trade",                  "renda_fixa",
        "Negócio a negócio",                              "Fixed income tick by tick",                        "18:00:00"),
    ReportDefinition("DebenturesBusiness",     "renda_fixa",
        "Negócios de VM de renda fixa no Puma",           "Fixed income securities trades conducted on Puma", "20:00:00"),
    ReportDefinition("Register",               "renda_fixa",
        "Registro",                                       "Register",                                         "08:00:00"),

    # ── Renda variável — Resumo de ações ────────────────────────────────────
    ReportDefinition("DailyAverageStocks",          "renda_variavel",
        "Ações",                                          "Equities",                                         "20:00:00"),
    ReportDefinition("StocksOperationSummary",       "renda_variavel",
        "Ações - resumo das operações",                   "Equities - transactions summary",                  "20:00:00"),
    ReportDefinition("InstrumentsEquities",          "renda_variavel",
        "Cadastro de instrumentos",                       "Instruments consolidated",                         "21:00:00"),
    ReportDefinition("MarginScenarios",              "renda_variavel",
        "Cenários de margem para ativos líquidos",        "Margin scenarios for liquid assets",               "21:00:00"),
    ReportDefinition("IOPV",                         "renda_variavel",
        "Comportamento dos valores de referência ETFs",   "Indicative optimized portfolio value (IOPV) ETFs", "20:00:00"),
    ReportDefinition("ForwardMarket",                "renda_variavel",
        "Mercado a termo",                                "Forward market",                                   "20:00:00"),
    ReportDefinition("AverageChart",                 "renda_variavel",
        "Médias diárias - volume em um ano",              "Daily averages - annual volume (BRL million)",     "20:00:00"),
    ReportDefinition("NegotiStrategi",               "renda_variavel",
        "Negociação de estratégias",                      "Trading strategies",                               "20:00:00"),
    ReportDefinition("ConsolidatedTradesEquities",   "renda_variavel",
        "Negócios consolidados do pregão",                "Consolidated trades of the session",               "08:00:00"),
    ReportDefinition("ConsolidatedTradesRVAfter",    "renda_variavel",
        "Negócios consolidados do pregão não regular",    "Consolidated trades of the non-regular session",   "21:00:00"),

    # ── Renda variável — Indicadores e informativos ──────────────────────────
    ReportDefinition("PreviaQuadrimestral",          "renda_variavel",
        "Composição das carteiras dos índices",           "Composition of indices' portfolios",               "21:00:00"),
    ReportDefinition("CNLDistrionCoffeLocation",     "renda_variavel",
        "Distribuição locais formação lotes café Conilon","Conilon coffee price formation centers",           "20:00:00"),
    ReportDefinition("DistrionCoffeLocation",        "renda_variavel",
        "Distribuição locais formação lotes café 4/5",    "Type 4/5 coffee price formation centers",          "20:00:00"),
    ReportDefinition("HarvestCoffeApprov",           "renda_variavel",
        "Estatísticas de safras - café Arábica 4/5",      "Crop statistics - arabica coffee 4/5",             "20:00:00"),
    ReportDefinition("CNLHarvestCoffeApprov",        "renda_variavel",
        "Estatísticas de safras - café Conilon",          "Crop statistics - Conilon coffee",                 "20:00:00"),
    ReportDefinition("INDEXES",                      "renda_variavel",
        "Evolução dos índices",                           "Indexes evolution",                                "20:00:00"),
    ReportDefinition("HistoricalExchange",           "renda_variavel",
        "Histórico de taxas de câmbio (BCB nº 120)",      "Exchange rates history (Resolution BCB no. 120)",  "21:00:00"),
    ReportDefinition("EconomicIndicators",           "renda_variavel",
        "Indicadores econômicos",                         "Economic indicators",                              "08:00:00"),
    ReportDefinition("ValidLotsSettle",              "renda_variavel",
        "Lotes válidos para liquidação - café Arábica",   "Valid lots for contract settlement - arabica",     "20:00:00"),
    ReportDefinition("CNLValidLotsSettle",           "renda_variavel",
        "Lotes válidos para liquidação - café Conilon",   "Valid lots for contract settlement - Conilon",     "20:00:00"),
    ReportDefinition("SharesInvesVolum",             "renda_variavel",
        "Participação dos investidores",                  "Participation of investors",                       "20:00:00"),
    ReportDefinition("SharesInvesVolumMonthly",      "renda_variavel",
        "Participação dos investidores mensal",           "Participation of investors monthly",               ""),

    # ── Renda variável — Maiores oscilações ─────────────────────────────────
    ReportDefinition("IbovespaStockBiggestHighs",    "renda_variavel",
        "Ações do IBOVESPA - maiores altas",              "IBOVESPA equities - largest gains",                "08:00:00"),
    ReportDefinition("IbovespaStockBiggestLow",      "renda_variavel",
        "Ações do IBOVESPA - maiores baixas",             "IBOVESPA equities - largest losses",               "08:00:00"),
    ReportDefinition("Forward",                      "renda_variavel",
        "Ações mais negociadas - a termo",                "Most traded equities - forward",                   "08:00:00"),
    ReportDefinition("OptionsPurshase",              "renda_variavel",
        "Opções mais negociadas - compra",                "Most traded equities - call options",              "08:00:00"),
    ReportDefinition("OptionsSelling",               "renda_variavel",
        "Opções mais negociadas - venda",                 "Most traded equities - put options",               "08:00:00"),
    ReportDefinition("InCash",                       "renda_variavel",
        "Ações mais negociadas - à vista",                "Most traded equities - cash",                      "08:00:00"),
    ReportDefinition("InCashMarketBiggestHighs",     "renda_variavel",
        "Mercado à vista - maiores altas",                "Cash market - largest gains",                      "08:00:00"),
    ReportDefinition("InCashMarketBiggestLow",       "renda_variavel",
        "Mercado à vista - maiores baixas",               "Cash market - largest losses",                     "08:00:00"),

    # ── Derivativos — Resumo ─────────────────────────────────────────────────
    ReportDefinition("DailyAverageDerivatives2",     "derivativos",
        "Derivativos",                                    "Derivatives",                                      "06:00:00"),
    ReportDefinition("DerivativesMtM",               "derivativos",
        "Derivativos - mark to market",                   "Derivatives - mark to market",                     "20:00:00"),
    ReportDefinition("DerivativesOperation2",        "derivativos",
        "Derivativos - resumo das operações",             "Derivatives - transactions summary",               "08:00:00"),

    # ── Derivativos — Balcão ─────────────────────────────────────────────────
    ReportDefinition("OTCInventoryCCP",              "derivativos",
        "Estoque com CCP",                                "Inventory with CCP",                               "08:00:00"),
    ReportDefinition("OTCInventoryWCCP",             "derivativos",
        "Estoque sem CCP",                                "Inventory without CCP",                            "08:00:00"),
    ReportDefinition("FlexibleOptions",              "derivativos",
        "Opções flexíveis",                               "Flexible options",                                 "03:00:00"),
    ReportDefinition("OTCRegistrationCCP",           "derivativos",
        "Registro com CCP",                               "Record with CCP",                                  "08:00:00"),
    ReportDefinition("OTCRegistrationWCCP",          "derivativos",
        "Registro sem CCP",                               "Record without CCP",                               "08:00:00"),
    ReportDefinition("SwapFlex",                     "derivativos",
        "Swap",                                           "Swap",                                             "03:00:00"),

    # ── Derivativos — Bolsa ──────────────────────────────────────────────────
    ReportDefinition("InstrumentsDerivatives",            "derivativos",
        "Cadastro de instrumentos",                       "Instruments consolidated",                         "21:00:00"),
    ReportDefinition("ConsolidatedTradesDerivatives",     "derivativos",
        "Negócios consolidados do pregão",                "Consolidated trades of the session",               "21:00:00"),
    ReportDefinition("ConsolidatedTradesDerivativesAfter","derivativos",
        "Negócios consolidados do pregão não regular",    "Consolidated trades of the non-regular session",   "21:00:00"),
    ReportDefinition("OpenPositionsEquities",             "derivativos",
        "Posições em aberto",                             "Open positions",                                   "21:00:00"),

    # ── Outros dados — Clearing e depositária ───────────────────────────────
    ReportDefinition("Custody",                      "outros_dados",
        "Ações custodiadas - programa de ADR",            "Equities in custody - ADR program",                "07:00:00"),
    ReportDefinition("ProventionCreditVariable",     "outros_dados",
        "Crédito de proventos",                           "Earnings credited",                                "00:00:00"),
    ReportDefinition("FugibleCustody",               "outros_dados",
        "Custódia fungível",                              "Fungible custody",                                 "00:00:00"),
    ReportDefinition("DeadlineDepositSecurities",    "outros_dados",
        "Prazo para depósito de títulos",                 "Period to deposit securities",                     "00:00:00"),

    # ── Outros dados — Empréstimo de ativos ─────────────────────────────────
    ReportDefinition("BTBLoanBalance",               "outros_dados",
        "Empréstimos registrados",                        "Registered securities lending",                    "08:00:00"),
    ReportDefinition("BTBTrade",                     "outros_dados",
        "Negócios",                                       "Trades",                                           "21:00:00"),
    ReportDefinition("BTBLendingOpenPosition",       "outros_dados",
        "Posições em aberto",                             "Open positions",                                   "08:00:00"),
    ReportDefinition("Renewals",                     "outros_dados",
        "Renovações",                                     "Renewals",                                         "21:00:00"),

    # ── Outros dados — COE ──────────────────────────────────────────────────
    ReportDefinition("COEInventory",                 "outros_dados",
        "Estoque",                                        "Inventory",                                        "08:00:00"),
    ReportDefinition("COERegistration",              "outros_dados",
        "Registro",                                       "Records",                                          "08:00:00"),
)

# Pre-built lookup by api_name for O(1) access.
CATALOG_BY_NAME: Final[dict[str, ReportDefinition]] = {
    r.api_name: r for r in CATALOG
}

# Enabled subset — what the pipeline iterates over.
ENABLED_REPORTS: Final[tuple[ReportDefinition, ...]] = tuple(
    r for r in CATALOG if r.enabled
)