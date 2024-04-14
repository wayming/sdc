package collector

import "reflect"

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
	Symbol            string  `json:"symbol"`
	Volume            float64 `json:"volume"`
}

type FinancialsIncome struct {
	CostOfRevenue            float64 `json:"cost_of_revenue"`
	DepreciationAmortization float64 `json:"depreciation_amortization"`
	DividendGrowth           float64 `json:"dividend_growth"`
	DividendPerShare         float64 `json:"dividend_per_share"`
	EBIT                     float64 `json:"ebit"`
	EBITMargin               float64 `json:"ebit_margin"`
	EBITDA                   float64 `json:"ebitda"`
	EBITDAMargin             float64 `json:"ebitda_margin"`
	EffectiveTaxRate         float64 `json:"effective_tax_rate"`
	EPSBasic                 float64 `json:"eps_basic"`
	EPSDiluted               float64 `json:"eps_diluted"`
	EPSGrowth                float64 `json:"eps_growth"`
	FreeCashFlow             float64 `json:"free_cash_flow"`
	FreeCashFlowMargin       float64 `json:"free_cash_flow_margin"`
	FreeCashFlowPerShare     float64 `json:"free_cash_flow_per_share"`
	GrossMargin              float64 `json:"gross_margin"`
	GrossProfit              float64 `json:"gross_profit"`
	IncomeTax                float64 `json:"income_tax"`
	InterestExpenseIncome    float64 `json:"interest_expense_income"`
	NetIncome                float64 `json:"net_income"`
	NetIncomeCommon          float64 `json:"net_income_common"`
	NetIncomeGrowth          float64 `json:"net_income_growth"`
	OperatingExpenses        float64 `json:"operating_expenses"`
	OperatingIncome          float64 `json:"operating_income"`
	OperatingMargin          float64 `json:"operating_margin"`
	OtherExpenseIncome       float64 `json:"other_expense_income"`
	OtherOperatingExpenses   float64 `json:"other_operating_expenses"`
	PreferredDividends       float64 `json:"preferred_dividends"`
	PretaxIncome             float64 `json:"pretax_income"`
	ProfitMargin             float64 `json:"profit_margin"`
	QuarterEnded             string  `json:"quarter_ended"`
	ResearchDevelopment      float64 `json:"research_development"`
	Revenue                  float64 `json:"revenue"`
	RevenueGrowthYOY         float64 `json:"revenue_growth_yoy"`
	SellingGeneralAdmin      float64 `json:"selling_general_admin"`
	SharesChange             float64 `json:"shares_change"`
	SharesOutstandingBasic   float64 `json:"shares_outstanding_basic"`
	SharesOutstandingDiluted float64 `json:"shares_outstanding_diluted"`
	Symbol                   string  `json:"symbol"`
}

type FinancialsBalanceShet struct {
	AccountsPayable          float64 `json:"accounts_payable"`
	BookValuePerShare        float64 `json:"book_value_per_share"`
	CashCashEquivalents      float64 `json:"cash_cash_equivalents"`
	CashEquivalents          float64 `json:"cash_equivalents"`
	CashGrowth               float64 `json:"cash_growth"`
	ComprehensiveIncome      float64 `json:"comprehensive_income"`
	CurrentDebt              float64 `json:"current_debt"`
	DebtGrowth               float64 `json:"debt_growth"`
	DeferredRevenue          float64 `json:"deferred_revenue"`
	GoodwillAndIntangibles   float64 `json:"goodwill_and_intangibles"`
	Inventory                float64 `json:"inventory"`
	LongTermDebt             float64 `json:"long_term_debt"`
	LongTermInvestments      float64 `json:"long_term_investments"`
	NetCashDebt              float64 `json:"net_cash_debt"`
	NetCashDebtGrowth        float64 `json:"net_cash_debt_growth"`
	NetCashPerShare          float64 `json:"net_cash_per_share"`
	OtherCurrentAssets       float64 `json:"other_current_assets"`
	OtherCurrentLiabilities  float64 `json:"other_current_liabilities"`
	OtherLongTermAssets      float64 `json:"other_long_term_assets"`
	OtherLongTermLiabilities float64 `json:"other_long_term_liabilities"`
	PropertyPlantEquipment   float64 `json:"property_plant_equipment"`
	QuarterEnded             string  `json:"quarter_ended"`
	Receivables              float64 `json:"receivables"`
	RetainedEarnings         float64 `json:"retained_earnings"`
	ShareholdersEquity       float64 `json:"shareholders_equity"`
	Symbol                   string  `json:"symbol"`
	ShortTermInvestments     float64 `json:"short_term_investments"`
	TotalAssets              float64 `json:"total_assets"`
	TotalCurrentAssets       float64 `json:"total_current_assets"`
	TotalCurrentLiabilities  float64 `json:"total_current_liabilities"`
	TotalDebt                float64 `json:"total_debt"`
	TotalLiabilities         float64 `json:"total_liabilities"`
	TotalLongTermAssets      float64 `json:"total_long_term_assets"`
	TotalLongTermLiabilities float64 `json:"total_long_term_liabilities"`
	WorkingCapital           float64 `json:"working_capital"`
}

type FinancialsCashFlow struct {
	Acquisitions             float64 `json:"acquisitions"`
	CaptialExpenditures      float64 `json:"capital_expenditures"`
	ChangeInInvestments      float64 `json:"change_in_investments"`
	CostOfRevenue            float64 `json:"cost_of_revenue"`
	DebtIssuedPaid           float64 `json:"debt_issued_paid"`
	DepreciationAmortization float64 `json:"depreciation_amortization"`
	DividendGrowth           float64 `json:"dividend_growth"`
	DividendPerShare         float64 `json:"dividend_per_share"`
	DividendsPaid            float64 `json:"dividends_paid"`
	EBIT                     float64 `json:"ebit"`
	EBITMargin               float64 `json:"ebit_margin"`
	EBITDA                   float64 `json:"ebitda"`
	EBITDAMargin             float64 `json:"ebitda_margin"`
	EffectiveTaxRate         float64 `json:"effective_tax_ratsde"`
	EPSBasic                 float64 `json:"eps_basic"`
	EPSDiluted               float64 `json:"eps_diluted"`
	EPSGrowth                float64 `json:"eps_growth"`
	ExchangeRateEffect       float64 `json:"exchange_rate_effect"`
	FinancinCashFlow         float64 `json:"financing_cash_flow"`
	FreeCashFlow             float64 `json:"free_cash_flow"`
	FreeCashFlowGrowth       float64 `json:"free_cash_flow_growth"`
	FreeCashFlowMargin       float64 `json:"free_cash_flow_margin"`
	FreeCashFlowPerShare     float64 `json:"free_cash_flow_per_share"`
	GrossMargin              float64 `json:"gross_margin"`
	GrossProfit              float64 `json:"gross_profit"`
	IncomeTax                float64 `json:"income_tax"`
	InvestingCashFlow        float64 `json:"investing_cash_flow"`
	InterestExpenseIncome    float64 `json:"interest_expense_income"`
	NetCashFlow              float64 `json:"net_cash_flow"`
	NetIncome                float64 `json:"net_income"`
	NetIncomeGrowth          float64 `json:"net_income_growth"`
	OperatingCashFlow        float64 `json:"operating_cash_flow"`
	OperatingCashFlowGrowth  float64 `json:"operating_cash_flow_growth"`
	OperatingExpenses        float64 `json:"operating_expenses"`
	OperatingIncome          float64 `json:"operating_income"`
	OperatingMargin          float64 `json:"operating_margin"`
	OtherExpenseIncome       float64 `json:"other_expense_income"`
	OtherFinancingActivities float64 `json:"other_financing_activities"`
	OtherInvestinActivities  float64 `json:"other_investing_activities"`
	OtherOperatingActivities float64 `json:"other_operating_activities"`
	OtherOperatingExpenses   float64 `json:"other_operating_expenses"`
	PretaxIncome             float64 `json:"pretax_income"`
	ProfitMargin             float64 `json:"profit_margin"`
	QuarterEnded             string  `json:"quarter_ended"`
	ResearchDevelopment      float64 `json:"research_development"`
	Revenue                  float64 `json:"revenue"`
	RevenueGrowthYOY         float64 `json:"revenue_growth_yoy"`
	Symbol                   string  `json:"symbol"`
	SellingGeneralAdmin      float64 `json:"selling_general_admin"`
	ShareBasedCompensation   float64 `json:"share_based_compensation"`
	ShareIssuanceRepurchase  float64 `json:"share_issuance_repurchase"`
	SharesChange             float64 `json:"shares_change"`
	SharesOutstandingBasic   float64 `json:"shares_outstanding_basic"`
	SharesOutstandingDiluted float64 `json:"shares_outstanding_diluted"`
}

type FinancialRatios struct {
	BuybackYieldDilution   float64 `json:"buyback_yield_dilution"`
	CurrentRatio           float64 `json:"current_ratio"`
	DebtEquityRatio        float64 `json:"debt_equity_ratio"`
	DividendYield          float64 `json:"dividend_yield"`
	EnterpriseValue        float64 `json:"enterprise_value"`
	InterestCoverage       float64 `json:"interest_coverage"`
	MarketCapGrowth        float64 `json:"market_cap_growth"`
	MarketCapitalization   float64 `json:"market_capitalization"`
	PFCFRatio              float64 `json:"p_fcf_ratio"`
	POCFRatio              float64 `json:"p_ocf_ratio"`
	PayoutRatio            float64 `json:"payout_ratio"`
	PBRatio                float64 `json:"pb_ratio"`
	PERatio                float64 `json:"pe_ratio"`
	PSRatio                float64 `json:"ps_ratio"`
	QuarterEnded           string  `json:"quarter_ended"`
	QuickRatio             float64 `json:"quick_ratio"`
	ReturnOnCapitalROIC    float64 `json:"return_on_capital_roic"`
	Symbol                 string  `json:"symbol"`
	TotalShareholderReturn float64 `json:"total_shareholder_return"`
}

func AllSAMetricsFields() map[string]map[string]JsonFieldMetadata {
	saStructTypes := []reflect.Type{
		reflect.TypeFor[StockOverview](),
		reflect.TypeFor[FinancialsIncome](),
		reflect.TypeFor[FinancialsBalanceShet](),
		reflect.TypeFor[FinancialsCashFlow](),
		reflect.TypeFor[FinancialRatios](),
	}

	allMetricsFields := make(map[string]map[string]JsonFieldMetadata)
	for _, structType := range saStructTypes {
		allMetricsFields[structType.Name()] = GetJsonStructMetadata(structType)
	}

	return allMetricsFields
}
