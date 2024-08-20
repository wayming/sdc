package collector

import (
	"reflect"
	"time"
)

const SA_REDIRECTED_SYMBOLS = "SARedirectedSymbols"
const SA_STOCKOVERVIEW = "SAStockOverview"
const SA_FINANCIALSINCOME = "SAFinancialsIncome"
const SA_FINANCIALSBALANCESHEET = "SAFinancialsBalanceSheet"
const SA_FINANCIALSCASHFLOW = "SAFinancialsCashFlow"
const SA_FINANCIALRATIOS = "SAFinancialRatios"
const SA_ANALYSTSRATING = "SAAnalystsRating"

type RedirectedSymbols struct {
	Symbol           string `json:"symbol" db:"PrimaryKey"`
	RedirectedSymbol string `json:"redirected_symbol"`
}

type StockOverview struct {
	FiftyTwoWeekRange string  `json:"52_week_range"`
	Analysts          string  `json:"analysts"`
	Beta              float64 `json:"beta"`
	DaysRange         string  `json:"days_range"`
	Dividend          string  `json:"dividend"`
	EarningsDate      string  `json:"earnings_date"`
	EPSTTM            float64 `json:"eps_ttm"`
	ExDividendDate    string  `json:"ex_dividend_date"`
	ForwardPE         float64 `json:"forward_pe"`
	MarketCap         float64 `json:"market_cap"`
	NetIncomeTTM      string  `json:"net_income_ttm"`
	Open              float64 `json:"open"`
	PERatio           float64 `json:"pe_ratio"`
	PreviousClose     float64 `json:"previous_close"`
	PriceTarget       float64 `json:"price_target"`
	RevenueTTM        float64 `json:"revenue_ttm"`
	SharesOut         float64 `json:"shares_out"`
	Symbol            string  `json:"symbol" db:"PrimaryKey"`
	Volume            float64 `json:"volume"`
}

type FinancialsIncome struct {
	CostOfRevenue                   float64   `json:"cost_of_revenue"`
	CurrencyExchangeGainLoss        float64   `json:"currency_exchange_gain_loss"`
	DAForEBITDA                     float64   `json:"depreciation_amortization"`
	DepreciationAmortization        float64   `json:"d_a_for_ebitda"`
	DividendGrowth                  float64   `json:"dividend_growth"`
	DividendPerShare                float64   `json:"dividend_per_share"`
	EBIT                            float64   `json:"ebit"`
	EBITMargin                      float64   `json:"ebit_margin"`
	EBITDA                          float64   `json:"ebitda"`
	EBITDAMargin                    float64   `json:"ebitda_margin"`
	EBTExcludingUnusualItems        float64   `json:"ebt_excluding_unusual_items"`
	EffectiveTaxRate                float64   `json:"effective_tax_rate"`
	EPSBasic                        float64   `json:"eps_basic"`
	EPSDiluted                      float64   `json:"eps_diluted"`
	EPSGrowth                       float64   `json:"eps_growth"`
	FreeCashFlow                    float64   `json:"free_cash_flow"`
	FreeCashFlowMargin              float64   `json:"free_cash_flow_margin"`
	FreeCashFlowPerShare            float64   `json:"free_cash_flow_per_share"`
	GainLossOnSaleOfInvestments     float64   `json:"gain_loss_on_sale_of_investments"`
	GrossMargin                     float64   `json:"gross_margin"`
	GrossProfit                     float64   `json:"gross_profit"`
	IncomeTaxExpense                float64   `json:"income_tax_expense"`
	InterestInvestmentIncome        float64   `json:"interest_investment_income"`
	InterestExpense                 float64   `json:"interest_expense"`
	InterestIncome                  float64   `json:"interest_income"`
	MergerRestructuringCharges      float64   `json:"merger_restructuring_charges"`
	NetIncome                       float64   `json:"net_income"`
	NetIncomeToCommon               float64   `json:"net_income_to_common"`
	NetIncomeGrowth                 float64   `json:"net_income_growth"`
	OperatingExpenses               float64   `json:"operating_expenses"`
	OperatingIncome                 float64   `json:"operating_income"`
	OperatingMargin                 float64   `json:"operating_margin"`
	OtherNonOperatingIncomeExpenses float64   `json:"other_non_operating_income_expenses"`
	PreferredDividends              float64   `json:"preferred_dividends"`
	PretaxIncome                    float64   `json:"pretax_income"`
	ProfitMargin                    float64   `json:"profit_margin"`
	QuarterEnding                   time.Time `json:"quarter_ending" db:"PrimaryKey"`
	ResearchDevelopment             float64   `json:"research_development"`
	Revenue                         float64   `json:"revenue"`
	RevenueGrowthYOY                float64   `json:"revenue_growth_yoy"`
	SellingGeneralAdmin             float64   `json:"selling_general_admin"`
	SharesChangeYoY                 float64   `json:"shares_change_yoy"`
	SharesOutstandingBasic          float64   `json:"shares_outstanding_basic"`
	SharesOutstandingDiluted        float64   `json:"shares_outstanding_diluted"`
	Symbol                          string    `json:"symbol" db:"PrimaryKey"`
}

type FinancialsBalanceSheet struct {
	AccountsPayable              float64   `json:"accounts_payable"`
	AccountsReceivable           float64   `json:"accounts_receivable"`
	AccruedExpenses              float64   `json:"accrued_expenses"`
	AdditionalPaidInCapital      float64   `json:"additional_paid_in_capital"`
	BookValuePerShare            float64   `json:"book_value_per_share"`
	Buildings                    float64   `json:"buildings"`
	CashEquivalents              float64   `json:"cash_equivalents"`
	CashGrowth                   float64   `json:"cash_growth"`
	CashShortTermInvestments     float64   `json:"cash_short_term_investments"`
	CommonStock                  float64   `json:"common_stock"`
	ComprensiveIncomeOther       float64   `json:"comprensive_income_other"`
	ConstructionInProgress       float64   `json:"construction_in_progress"`
	CurrentDebt                  float64   `json:"current_debt"`
	CurrentIncomeTaxesPayable    float64   `json:"current_income_taxes_payable"`
	CurrentPortionOfLeases       float64   `json:"current_portion_of_leases"`
	CurrentUnearnedRevenue       float64   `json:"current_unearned_revenue"`
	DebtGrowth                   float64   `json:"debt_growth"`
	DeferredRevenue              float64   `json:"deferred_revenue"`
	FillingDateSharesOutstanding float64   `json:"filing_date_shares_outstanding"`
	Goodwill                     float64   `json:"goodwill"`
	GoodwillAndIntangibles       float64   `json:"goodwill_and_intangibles"`
	IntangibleAssets             float64   `json:"intangible_assets"`
	Inventory                    float64   `json:"inventory"`
	Land                         float64   `json:"land"`
	LeaseholdImprovements        float64   `json:"leasehold_improvements"`
	LongTermDebt                 float64   `json:"long_term_debt"`
	LongTermInvestments          float64   `json:"long_term_investments"`
	LongTermLeases               float64   `json:"long_term_leases"`
	LongTermUneanedRevenue       float64   `json:"long_term_unearned_revenue"`
	Machinery                    float64   `json:"machinery"`
	NetCashDebt                  float64   `json:"net_cash_debt"`
	NetCashGrowth                float64   `json:"net_cash_growth"`
	NetCashPerShare              float64   `json:"net_cash_per_share"`
	OtherCurrentAssets           float64   `json:"other_current_assets"`
	OtherCurrentLiabilities      float64   `json:"other_current_liabilities"`
	OtherIntangibleAssets        float64   `json:"other_intangible_assets"`
	OtherLongTermAssets          float64   `json:"other_long_term_assets"`
	OtherLongTermLiabilities     float64   `json:"other_long_term_liabilities"`
	PropertyPlantEquipment       float64   `json:"property_plant_equipment"`
	PrepaidExpenses              float64   `json:"prepaid_expenses"`
	QuarterEnding                time.Time `json:"quarter_ending" db:"PrimaryKey"`
	Receivables                  float64   `json:"receivables"`
	RestrictedCash               float64   `json:"restricted_cash"`
	RetainedEarnings             float64   `json:"retained_earnings"`
	ShareholdersEquity           float64   `json:"shareholders_equity"`
	Symbol                       string    `json:"symbol" db:"PrimaryKey"`
	ShortTermInvestments         float64   `json:"short_term_investments"`
	TangibleBookValue            float64   `json:"tangible_book_value"`
	TangibleBookValuePerShare    float64   `json:"tangible_book_value_per_share"`
	TotalAssets                  float64   `json:"total_assets"`
	TotalCommonSharesOutstanding float64   `json:"total_common_shares_outstanding"`
	TotalCurrentAssets           float64   `json:"total_current_assets"`
	TotalCurrentLiabilities      float64   `json:"total_current_liabilities"`
	TotalDebt                    float64   `json:"total_debt"`
	TotalLiabilities             float64   `json:"total_liabilities"`
	TotalLiabilitiesEquity       float64   `json:"total_liabilities_equity"`
	TotalLongTermAssets          float64   `json:"total_long_term_assets"`
	TotalLongTermLiabilities     float64   `json:"total_long_term_liabilities"`
	WorkingCapital               float64   `json:"working_capital"`
}

type FinancialsCashFlow struct {
	Acquisitions                     float64   `json:"acquisitions"`
	AssetWritedownRestructuringCosts float64   `json:"asset_writedown_restructuring_costs"`
	CaptialExpenditures              float64   `json:"capital_expenditures"`
	CashAcquistions                  float64   `json:"cash_acquisitions"`
	CashIncomeTaxPaid                float64   `json:"cash_income_tax_paid"`
	CashInterestPaid                 float64   `json:"cash_interest_paid"`
	ChangeInInvestments              float64   `json:"change_in_investments"`
	ChangeInAccountsReceivable       float64   `json:"change_in_accounts_receivable"`
	ChangeInAccountsPayable          float64   `json:"change_in_accounts_payable"`
	ChangeInNetWorkingCapital        float64   `json:"change_in_net_working_capital"`
	ChangeInOtherNetOperatingAssets  float64   `json:"change_in_other_net_operating_assets"`
	ChangeInUnearnedRevenue          float64   `json:"change_in_unearned_revenue"`
	CommonDividendsPaid              float64   `json:"common_dividends_paid"`
	CommonStockIssued                float64   `json:"common_stock_issued"`
	CostOfRevenue                    float64   `json:"cost_of_revenue"`
	DebtIssuedPaid                   float64   `json:"debt_issued_paid"`
	DepreciationAmortization         float64   `json:"depreciation_amortization"`
	DividendGrowth                   float64   `json:"dividend_growth"`
	DividendPerShare                 float64   `json:"dividend_per_share"`
	DividendsPaid                    float64   `json:"dividends_paid"`
	EBIT                             float64   `json:"ebit"`
	EBITMargin                       float64   `json:"ebit_margin"`
	EBITDA                           float64   `json:"ebitda"`
	EBITDAMargin                     float64   `json:"ebitda_margin"`
	EffectiveTaxRate                 float64   `json:"effective_tax_ratsde"`
	EPSBasic                         float64   `json:"eps_basic"`
	EPSDiluted                       float64   `json:"eps_diluted"`
	EPSGrowth                        float64   `json:"eps_growth"`
	ExchangeRateEffect               float64   `json:"exchange_rate_effect"`
	FinancinCashFlow                 float64   `json:"financing_cash_flow"`
	ForeignExchangeRateAdjustments   float64   `json:"foreign_exchange_rate_adjustments"`
	FreeCashFlow                     float64   `json:"free_cash_flow"`
	FreeCashFlowGrowth               float64   `json:"free_cash_flow_growth"`
	FreeCashFlowMargin               float64   `json:"free_cash_flow_margin"`
	FreeCashFlowPerShare             float64   `json:"free_cash_flow_per_share"`
	GrossMargin                      float64   `json:"gross_margin"`
	GrossProfit                      float64   `json:"gross_profit"`
	IncomeTax                        float64   `json:"income_tax"`
	InvestingCashFlow                float64   `json:"investing_cash_flow"`
	InvestmentInSecurities           float64   `json:"investment_in_securities"`
	InterestExpenseIncome            float64   `json:"interest_expense_income"`
	LeveredFreeCashFlow              float64   `json:"levered_free_cash_flow"`
	LongTermDebtRepaid               float64   `json:"long_term_debt_repaid"`
	LossGainFromSaleOfInvestments    float64   `json:"loss_gain_from_sale_of_investments"`
	NetCashFlow                      float64   `json:"net_cash_flow"`
	NetDebtIssuedRepaid              float64   `json:"net_debt_issued_repaid"`
	NetIncome                        float64   `json:"net_income"`
	NetIncomeGrowth                  float64   `json:"net_income_growth"`
	OperatingCashFlow                float64   `json:"operating_cash_flow"`
	OperatingCashFlowGrowth          float64   `json:"operating_cash_flow_growth"`
	OperatingExpenses                float64   `json:"operating_expenses"`
	OperatingIncome                  float64   `json:"operating_income"`
	OperatingMargin                  float64   `json:"operating_margin"`
	OtherExpenseIncome               float64   `json:"other_expense_income"`
	OtherFinancingActivities         float64   `json:"other_financing_activities"`
	OtherInvestinActivities          float64   `json:"other_investing_activities"`
	OtherOperatingActivities         float64   `json:"other_operating_activities"`
	OtherOperatingExpenses           float64   `json:"other_operating_expenses"`
	PretaxIncome                     float64   `json:"pretax_income"`
	ProfitMargin                     float64   `json:"profit_margin"`
	QuarterEnding                    time.Time `json:"quarter_ending" db:"PrimaryKey"`
	RepurchaseOfCommonStock          float64   `json:"repurchase_of_common_stock"`
	ResearchDevelopment              float64   `json:"research_development"`
	Revenue                          float64   `json:"revenue"`
	RevenueGrowthYOY                 float64   `json:"revenue_growth_yoy"`
	SaleOfPropertyPlantEquipment     float64   `json:"sale_of_property_plant_equipment"`
	SellingGeneralAdmin              float64   `json:"selling_general_admin"`
	ShareBasedCompensation           float64   `json:"share_based_compensation"`
	ShareIssuanceRepurchase          float64   `json:"share_issuance_repurchase"`
	SharesChange                     float64   `json:"shares_change"`
	SharesOutstandingBasic           float64   `json:"shares_outstanding_basic"`
	SharesOutstandingDiluted         float64   `json:"shares_outstanding_diluted"`
	ShareRepurchases                 float64   `json:"share_repurchases"`
	ShortTermDebtRepaid              float64   `json:"short_term_debt_repaid"`
	StockBasedCompensation           float64   `json:"stock_based_compensation"`
	Symbol                           string    `json:"symbol" db:"PrimaryKey"`
	TotalDebtIssued                  float64   `json:"total_debt_issued"`
	TotalDebtRepaid                  float64   `json:"total_debt_repaid"`
	UnleveredFreeCashFlow            float64   `json:"unlevered_free_cash_flow"`
}

type FinancialRatios struct {
	AssetTurnover          float64   `json:"asset_turnover"`
	BuybackYieldDilution   float64   `json:"buyback_yield_dilution"`
	CurrentRatio           float64   `json:"current_ratio"`
	DebtEbitdaRatio        float64   `json:"debt_ebitda_ratio"`
	DebtEquityRatio        float64   `json:"debt_equity_ratio"`
	DebtFcfRatio           float64   `json:"debt_fcf_ratio"`
	DividendYield          float64   `json:"dividend_yield"`
	EnterpriseValue        float64   `json:"enterprise_value"`
	EvEbitdaRatio          float64   `json:"ev_ebitda_ratio"`
	EvEbitRatio            float64   `json:"ev_ebit_ratio"`
	EvFcfRatio             float64   `json:"ev_fcf_ratio"`
	EvSalesRatio           float64   `json:"ev_sales_ratio"`
	EarningsYield          float64   `json:"earnings_yield"`
	FCFYield               float64   `json:"fcf_yield"`
	InterestCoverage       float64   `json:"interest_coverage"`
	LastClosePrice         float64   `json:"last_close_price"`
	MarketCapGrowth        float64   `json:"market_cap_growth"`
	MarketCapitalization   float64   `json:"market_capitalization"`
	PFCFRatio              float64   `json:"p_fcf_ratio"`
	POCFRatio              float64   `json:"p_ocf_ratio"`
	PayoutRatio            float64   `json:"payout_ratio"`
	PBRatio                float64   `json:"pb_ratio"`
	PERatio                float64   `json:"pe_ratio"`
	PSRatio                float64   `json:"ps_ratio"`
	QuarterEnding          time.Time `json:"quarter_ending" db:"PrimaryKey"`
	QuickRatio             float64   `json:"quick_ratio"`
	ReturnOnAssetsROA      float64   `json:"return_on_assets_roa"`
	ReturnOnCapitalROIC    float64   `json:"return_on_capital_roic"`
	ReturnOnEquityROE      float64   `json:"return_on_equity_roe"`
	Symbol                 string    `json:"symbol" db:"PrimaryKey"`
	TotalShareholderReturn float64   `json:"total_shareholder_return"`
}

type AnalystsRating struct {
	Symbol          string  `json:"symbol" db:"PrimaryKey"`
	TotalAnalysts   int64   `json:"total_analysts"`
	ConsensusRating string  `json:"consensus_rating"`
	PriceTarget     float64 `json:"price_target"`
	Upside          float64 `json:"upside"`
}

func AllSAMetricsFields() map[string]map[string]JsonFieldMetadata {
	saStructTypes := []reflect.Type{
		reflect.TypeFor[StockOverview](),
		reflect.TypeFor[FinancialsIncome](),
		reflect.TypeFor[FinancialsBalanceSheet](),
		reflect.TypeFor[FinancialsCashFlow](),
		reflect.TypeFor[FinancialRatios](),
		reflect.TypeFor[AnalystsRating](),
	}

	allMetricsFields := make(map[string]map[string]JsonFieldMetadata)
	for _, structType := range saStructTypes {
		allMetricsFields[structType.Name()] = GetJsonStructMetadata(structType)
	}

	return allMetricsFields
}

var SADataTables = map[string]string{
	SA_REDIRECTED_SYMBOLS:     "sa_redirected_symbols",
	SA_STOCKOVERVIEW:          "sa_stockoverview",
	SA_FINANCIALSINCOME:       "sa_financialsincome",
	SA_FINANCIALSBALANCESHEET: "sa_financialsbalancesheet",
	SA_FINANCIALSCASHFLOW:     "sa_financialscashflow",
	SA_FINANCIALRATIOS:        "sa_financialratios",
	SA_ANALYSTSRATING:         "sa_analystsrating",
}

var SADataTypes = map[string]reflect.Type{
	SA_REDIRECTED_SYMBOLS:     reflect.TypeFor[RedirectedSymbols](),
	SA_STOCKOVERVIEW:          reflect.TypeFor[StockOverview](),
	SA_FINANCIALSINCOME:       reflect.TypeFor[FinancialsIncome](),
	SA_FINANCIALSBALANCESHEET: reflect.TypeFor[FinancialsBalanceSheet](),
	SA_FINANCIALSCASHFLOW:     reflect.TypeFor[FinancialsCashFlow](),
	SA_FINANCIALRATIOS:        reflect.TypeFor[FinancialRatios](),
	SA_ANALYSTSRATING:         reflect.TypeFor[AnalystsRating](),
}
