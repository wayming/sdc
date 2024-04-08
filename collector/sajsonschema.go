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
	PriceTarget       string  `json:"price_target"`
	RevenueTTM        float64 `json:"revenue_ttm"`
	SharesOut         float64 `json:"shares_out"`
	Volume            float64 `json:"volume"`
}

type FinancialsIncome struct {
	CostOfRevenue            string `json:"cost_of_revenue"`
	DepreciationAmortization string `json:"depreciation_amortization"`
	DividendGrowth           string `json:"dividend_growth"`
	DividendPerShare         string `json:"dividend_per_share"`
	EBIT                     string `json:"ebit"`
	EBITMargin               string `json:"ebit_margin"`
	EBITDA                   string `json:"ebitda"`
	EBITDAMargin             string `json:"ebitda_margin"`
	EffectiveTaxRate         string `json:"effective_tax_rate"`
	EPSBasic                 string `json:"eps_basic"`
	EPSDiluted               string `json:"eps_diluted"`
	EPSGrowth                string `json:"eps_growth"`
	FreeCashFlow             string `json:"free_cash_flow"`
	FreeCashFlowMargin       string `json:"free_cash_flow_margin"`
	FreeCashFlowPerShare     string `json:"free_cash_flow_per_share"`
	GrossMargin              string `json:"gross_margin"`
	GrossProfit              string `json:"gross_profit"`
	IncomeTax                string `json:"income_tax"`
	InterestExpenseIncome    string `json:"interest_expense_income"`
	NetIncome                string `json:"net_income"`
	NetIncomeGrowth          string `json:"net_income_growth"`
	OperatingExpenses        string `json:"operating_expenses"`
	OperatingIncome          string `json:"operating_income"`
	OperatingMargin          string `json:"operating_margin"`
	OtherExpenseIncome       string `json:"other_expense_income"`
	OtherOperatingExpenses   string `json:"other_operating_expenses"`
	PretaxIncome             string `json:"pretax_income"`
	ProfitMargin             string `json:"profit_margin"`
	QuarterEnded             string `json:"quarter_ended"`
	ResearchDevelopment      string `json:"research_development"`
	Revenue                  string `json:"revenue"`
	RevenueGrowthYOY         string `json:"revenue_growth_yoy"`
	SellingGeneralAdmin      string `json:"selling,_general_admin"`
	SharesChange             string `json:"shares_change"`
	SharesOutstandingBasic   string `json:"shares_outstanding_basic"`
	SharesOutstandingDiluted string `json:"shares_outstanding_diluted"`
}

type FinancialsBalanceShet struct {
	AccountsPayable          string `json:"accounts_payable"`
	BookValuePerShare        string `json:"book_value_per_share"`
	CashCashEquivalents      string `json:"cash_cash_equivalents"`
	CashEquivalents          string `json:"cash_equivalents"`
	CashGrowth               string `json:"cash_growth"`
	ComprehensiveIncome      string `json:"comprehensive_income"`
	CurrentDebt              string `json:"current_debt"`
	DebtGrowth               string `json:"debt_growth"`
	DeferredRevenue          string `json:"deferred_revenue"`
	GoodwillAndIntangibles   string `json:"goodwill_and_intangibles"`
	Inventory                string `json:"inventory"`
	LongTermDebt             string `json:"long_term_debt"`
	LongTermInvestments      string `json:"long_term_investments"`
	NetCashDebt              string `json:"net_cash_debt"`
	NetCashDebtGrowth        string `json:"net_cash_debt_growth"`
	NetCashPerShare          string `json:"net_cash_per_share"`
	OtherCurrentAssets       string `json:"other_current_assets"`
	OtherCurrentLiabilities  string `json:"other_current_liabilities"`
	OtherLongTermAssets      string `json:"other_long_term_assets"`
	OtherLongTermLiabilities string `json:"other_long_term_liabilities"`
	PropertyPlantEquipment   string `json:"property,_plant_equipment"`
	QuarterEnded             string `json:"quarter_ended"`
	Receivables              string `json:"receivables"`
	RetainedEarnings         string `json:"retained_earnings"`
	ShareholdersEquity       string `json:"shareholders_equity"`
	TotalAssets              string `json:"total_assets"`
	TotalCurrentAssets       string `json:"total_current_assets"`
	TotalCurrentLiabilities  string `json:"total_current_liabilities"`
	TotalDebt                string `json:"total_debt"`
	TotalLiabilities         string `json:"total_liabilities"`
	TotalLongTermAssets      string `json:"total_long_term_assets"`
	TotalLongTermLiabilities string `json:"total_long_term_liabilities"`
	WorkingCapital           string `json:"working_capital"`
}

type FinancialsCashFlow struct {
	CostOfRevenue            string `json:"cost_of_revenue"`
	DepreciationAmortization string `json:"depreciation_amortization"`
	DividendGrowth           string `json:"dividend_growth"`
	DividendPerShare         string `json:"dividend_per_share"`
	EBIT                     string `json:"ebit"`
	EBITMargin               string `json:"ebit_margin"`
	EBITDA                   string `json:"ebitda"`
	EBITDAMargin             string `json:"ebitda_margin"`
	EffectiveTaxRate         string `json:"effective_tax_rate"`
	EPSBasic                 string `json:"eps_basic"`
	EPSDiluted               string `json:"eps_diluted"`
	EPSGrowth                string `json:"eps_growth"`
	FreeCashFlow             string `json:"free_cash_flow"`
	FreeCashFlowMargin       string `json:"free_cash_flow_margin"`
	FreeCashFlowPerShare     string `json:"free_cash_flow_per_share"`
	GrossMargin              string `json:"gross_margin"`
	GrossProfit              string `json:"gross_profit"`
	IncomeTax                string `json:"income_tax"`
	InterestExpenseIncome    string `json:"interest_expense_income"`
	NetIncome                string `json:"net_income"`
	NetIncomeGrowth          string `json:"net_income_growth"`
	OperatingExpenses        string `json:"operating_expenses"`
	OperatingIncome          string `json:"operating_income"`
	OperatingMargin          string `json:"operating_margin"`
	OtherExpenseIncome       string `json:"other_expense_income"`
	OtherOperatingExpenses   string `json:"other_operating_expenses"`
	PretaxIncome             string `json:"pretax_income"`
	ProfitMargin             string `json:"profit_margin"`
	QuarterEnded             string `json:"quarter_ended"`
	ResearchDevelopment      string `json:"research_development"`
	Revenue                  string `json:"revenue"`
	RevenueGrowthYOY         string `json:"revenue_growth_yoy"`
	SellingGeneralAdmin      string `json:"selling,_general_admin"`
	SharesChange             string `json:"shares_change"`
	SharesOutstandingBasic   string `json:"shares_outstanding_basic"`
	SharesOutstandingDiluted string `json:"shares_outstanding_diluted"`
}

type FinancialRatios struct {
	BuybackYieldDilution   string `json:"buyback_yield_dilution"`
	CurrentRatio           string `json:"current_ratio"`
	DebtEquityRatio        string `json:"debt_equity_ratio"`
	DividendYield          string `json:"dividend_yield"`
	EnterpriseValue        string `json:"enterprise_value"`
	InterestCoverage       string `json:"interest_coverage"`
	MarketCapGrowth        string `json:"market_cap_growth"`
	MarketCapitalization   string `json:"market_capitalization"`
	PFCFRatio              string `json:"p_fcf_ratio"`
	POCFRatio              string `json:"p_ocf_ratio"`
	PayoutRatio            string `json:"payout_ratio"`
	PBRatio                string `json:"pb_ratio"`
	PERatio                string `json:"pe_ratio"`
	PSRatio                string `json:"ps_ratio"`
	QuarterEnded           string `json:"quarter_ended"`
	QuickRatio             string `json:"quick_ratio"`
	ReturnOnCapitalROIC    string `json:"return_on_capital_roic"`
	TotalShareholderReturn string `json:"total_shareholder_return"`
}

func AllSAMetricsFields() map[string]map[string]reflect.Type {
	saStructTypes := []reflect.Type{
		reflect.TypeFor[StockOverview](),
	}

	allMetricsFields := make(map[string]map[string]reflect.Type)
	for _, structType := range saStructTypes {
		allMetricsFields[structType.Name()] = JsonStructFieldTypeMap(structType)
	}

	return allMetricsFields
}
