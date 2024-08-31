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
	AdvertisingExpenses                    float64   `json:"advertising_expenses"`
	CostOfRevenue                          float64   `json:"cost_of_revenue"`
	CurrencyExchangeGainLoss               float64   `json:"currency_exchange_gain_loss"`
	DAForEBITDA                            float64   `json:"depreciation_amortization"`
	DepreciationAmortization               float64   `json:"d_a_for_ebitda"`
	DividendGrowth                         float64   `json:"dividend_growth"`
	DividendPerShare                       float64   `json:"dividend_per_share"`
	EarningsFromContinuingOperations       float64   `json:"earnings_from_continuing_operations"`
	EarningsFromDiscontinuedOperations     float64   `json:"earnings_from_discontinued_operations"`
	EarningsFromEquityInvestments          float64   `json:"earnings_from_equity_investments"`
	EBIT                                   float64   `json:"ebit"`
	EBITMargin                             float64   `json:"ebit_margin"`
	EBITDA                                 float64   `json:"ebitda"`
	EBITDAMargin                           float64   `json:"ebitda_margin"`
	EBTExcludingUnusualItems               float64   `json:"ebt_excluding_unusual_items"`
	EffectiveTaxRate                       float64   `json:"effective_tax_rate"`
	EPSBasic                               float64   `json:"eps_basic"`
	EPSDiluted                             float64   `json:"eps_diluted"`
	EPSGrowth                              float64   `json:"eps_growth"`
	FreeCashFlow                           float64   `json:"free_cash_flow"`
	FreeCashFlowMargin                     float64   `json:"free_cash_flow_margin"`
	FreeCashFlowPerShare                   float64   `json:"free_cash_flow_per_share"`
	GainLossOnSaleOfAssets                 float64   `json:"gain_loss_on_sale_of_assets"`
	GainLossOnSaleOfInvestments            float64   `json:"gain_loss_on_sale_of_investments"`
	GrossMargin                            float64   `json:"gross_margin"`
	GrossProfit                            float64   `json:"gross_profit"`
	ImpairmentOfGoodwill                   float64   `json:"impairment_of_goodwill"`
	IncomeTaxExpense                       float64   `json:"income_tax_expense"`
	InterestInvestmentIncome               float64   `json:"interest_investment_income"`
	InterestExpense                        float64   `json:"interest_expense"`
	InterestIncome                         float64   `json:"interest_income"`
	LegalSettlements                       float64   `json:"legal_settlements"`
	MergerRestructuringCharges             float64   `json:"merger_restructuring_charges"`
	MinorityInterestInEarnings             float64   `json:"minority_interest_in_earnings"`
	NetIncome                              float64   `json:"net_income"`
	NetIncomeToCommon                      float64   `json:"net_income_to_common"`
	NetIncomeToCompany                     float64   `json:"net_income_to_company"`
	NetIncomeGrowth                        float64   `json:"net_income_growth"`
	OperatingExpenses                      float64   `json:"operating_expenses"`
	OperatingIncome                        float64   `json:"operating_income"`
	OperatingMargin                        float64   `json:"operating_margin"`
	OperatingRevenue                       float64   `json:"operating_revenue"`
	OtherNonOperatingIncomeExpenses        float64   `json:"other_non_operating_income_expenses"`
	OtherOperatingExpenses                 float64   `json:"other_operating_expenses"`
	OtherUnusalItems                       float64   `json:"other_unusual_items"`
	OtherRevenue                           float64   `json:"other_revenue"`
	PreferredDividends                     float64   `json:"preferred_dividends"`
	PreferredDividendsOtherAdjustments     float64   `json:"preferred_dividends_other_adjustments"`
	PretaxIncome                           float64   `json:"pretax_income"`
	ProfitMargin                           float64   `json:"profit_margin"`
	PeriodEnding                           time.Time `json:"period_ending"`
	FiscalQuarter                          time.Time `json:"fiscal_quarter" db:"PrimaryKey"`
	ResearchDevelopment                    float64   `json:"research_development"`
	Revenue                                float64   `json:"revenue"`
	RevenueAsReported                      float64   `json:"revenue_as_reported"`
	RevenueGrowthYOY                       float64   `json:"revenue_growth_yoy"`
	SellingGeneralAdmin                    float64   `json:"selling_general_admin"`
	SharesChangeYoY                        float64   `json:"shares_change_yoy"`
	SharesOutstandingBasic                 float64   `json:"shares_outstanding_basic"`
	SharesOutstandingDiluted               float64   `json:"shares_outstanding_diluted"`
	Symbol                                 string    `json:"symbol" db:"PrimaryKey"`
	AssetWritedown                         float64   `json:"asset_writedown"`
	InterestAndDividendIncome              float64   `json:"interest_and_dividend_income"`
	TotalInterestExpense                   float64   `json:"total_interest_expense"`
	NetInterestInccome                     float64   `json:"net_interest_income"`
	RevenueBeforeLoanLosses                float64   `json:"revenue_before_loan_losses"`
	MortgageBanking                        float64   `json:"mortgage_banking"`
	PremiumsAnnuityRevenue                 float64   `json:"premiums_annuity_revenue"`
	GainLossOnSaleOfLoansReceivables       float64   `json:"gain_loss_on_sale_of_loans_receivables"`
	ProvisionForLoanLosses                 float64   `json:"provision_for_loan_losses"`
	RentalRevenue                          float64   `json:"rental_revenue"`
	TotalInterestDividendIncome            float64   `json:"total_interest_dividend_income"`
	SalariesEmployeeBenefits               float64   `json:"salaries_employee_benefits"`
	TotalRevenue                           float64   `json:"total_revenue"`
	CostOfServicesProvided                 float64   `json:"cost_of_services_provided"`
	PolicyBenefits                         float64   `json:"policy_benefits"`
	PropertyExpenses                       float64   `json:"property_expenses"`
	PolicyAcquisitionUnderwritingCosts     float64   `json:"policy_acquisition_underwriting_costs"`
	SellingGeneralAdministrative           float64   `json:"selling_general_administrative"`
	TotalOperatinExpenses                  float64   `json:"total_operating_expenses"`
	AmortizationOfGoodwillIntangibles      float64   `json:"amortization_of_goodwill_intangibles"`
	EarninsFromContinuingOps               float64   `json:"earnings_from_continuing_ops"`
	OtherNonOperatingIncome                float64   `json:"other_non_operating_income"`
	BasicSharesOutstanding                 float64   `json:"basic_shares_outstanding"`
	EarningsFromDiscontinuedOps            float64   `json:"earnings_from_discontinued_ops"`
	DilutedSharesOutstanding               float64   `json:"diluted_shares_outstanding"`
	FundsFromOperatioinsFfo                float64   `json:"funds_from_operations_ffo"`
	FfoPerShare                            float64   `json:"ffo_per_share"`
	AdjustedFundsFromOperationsAffo        float64   `json:"adjusted_funds_from_operations_affo"`
	AffoPerShare                           float64   `json:"affo_per_share"`
	FfoPayotRatio                          float64   `json:"ffo_payout_ratio"`
	InterestIncomeOnLoans                  float64   `json:"interest_income_on_loans"`
	InterestIncomeOnInvestments            float64   `json:"interest_income_on_investments"`
	TotalInterestIncome                    float64   `json:"total_interest_income"`
	InterestPaidOnDeposits                 float64   `json:"interest_paid_on_deposits"`
	InterestPaidOnBorrowings               float64   `json:"interest_paid_on_borrowings"`
	NetInterestIncomeGrowthYoy             float64   `json:"net_interest_income_growth_yoy"`
	MortgageBankingActivities              float64   `json:"mortgage_banking_activities"`
	TrustIncome                            float64   `json:"trust_income"`
	OtherNonInterestIncome                 float64   `json:"other_non_interest_income"`
	TotalNonInterestIncome                 float64   `json:"total_non_interest_income"`
	NonInterestIncomeGrowthYoy             float64   `json:"non_interest_income_growth_yoy"`
	RevenuesBeforeLoanLosses               float64   `json:"revenues_before_loan_losses"`
	SalariesAndEmployeeBenefits            float64   `json:"salaries_and_employee_benefits"`
	OccupancyExpenses                      float64   `json:"occupancy_expenses"`
	FederalDepositInsurance                float64   `json:"federal_deposit_insurance"`
	OtherNonInterestExpense                float64   `json:"other_non_interest_expense"`
	TotalNonInterestExpense                float64   `json:"total_non_interest_expense"`
	FuelPurchasedPower                     float64   `json:"fuel_purchased_power"`
	OperationsMaintenance                  float64   `json:"operations_maintenance"`
	NetInterestExpense                     float64   `json:"net_interest_expense"`
	IncomeLossOnEquityInvestments          float64   `json:"income_loss_on_equity_investments"`
	AllowanceForEquityFundsForConstruction float64   `json:"allowance_for_equity_funds_for_construction"`
	InsuranceSettlements                   float64   `json:"insurance_settlements"`
}

type FinancialsBalanceSheet struct {
	AccountsPayable                        float64   `json:"accounts_payable"`
	AccountsReceivable                     float64   `json:"accounts_receivable"`
	AccruedExpenses                        float64   `json:"accrued_expenses"`
	AdditionalPaidInCapital                float64   `json:"additional_paid_in_capital"`
	BookValuePerShare                      float64   `json:"book_value_per_share"`
	Buildings                              float64   `json:"buildings"`
	CashEquivalents                        float64   `json:"cash_equivalents"`
	CashGrowth                             float64   `json:"cash_growth"`
	CashShortTermInvestments               float64   `json:"cash_short_term_investments"`
	CommonStock                            float64   `json:"common_stock"`
	ComprensiveIncomeOther                 float64   `json:"comprensive_income_other"`
	ConstructionInProgress                 float64   `json:"construction_in_progress"`
	CurrentDebt                            float64   `json:"current_debt"`
	CurrentIncomeTaxesPayable              float64   `json:"current_income_taxes_payable"`
	CurrentPortionOfLeases                 float64   `json:"current_portion_of_leases"`
	CurrentPortionOLongTermDebt            float64   `json:"current_portion_of_long_term_debt"`
	CurrentUnearnedRevenue                 float64   `json:"current_unearned_revenue"`
	DebtGrowth                             float64   `json:"debt_growth"`
	DeferredRevenue                        float64   `json:"deferred_revenue"`
	FillingDateSharesOutstanding           float64   `json:"filing_date_shares_outstanding"`
	Goodwill                               float64   `json:"goodwill"`
	GoodwillAndIntangibles                 float64   `json:"goodwill_and_intangibles"`
	IntangibleAssets                       float64   `json:"intangible_assets"`
	Inventory                              float64   `json:"inventory"`
	InvestmentsInDebtSecurities            float64   `json:"investments_in_debt_securities"`
	InvestmentsInEquityPreferredSecurities float64   `json:"investments_in_equity_preferred_securities"`
	Land                                   float64   `json:"land"`
	LeaseholdImprovements                  float64   `json:"leasehold_improvements"`
	LoansLeaseReceivables                  float64   `json:"loans_lease_receivables"`
	LongTermAccountsReceivable             float64   `json:"long_term_accounts_receivable"`
	LongTermDebt                           float64   `json:"long_term_debt"`
	LongTermDeferredCharges                float64   `json:"long_term_deferred_charges"`
	LongTermDeferredTaxAssets              float64   `json:"long_term_deferred_tax_assets"`
	LongTermDeferredTaxLiabilities         float64   `json:"long_term_deferred_tax_liabilities"`
	LongTermInvestments                    float64   `json:"long_term_investments"`
	LongTermLeases                         float64   `json:"long_term_leases"`
	LongTermUneanedRevenue                 float64   `json:"long_term_unearned_revenue"`
	Machinery                              float64   `json:"machinery"`
	MinorityInterest                       float64   `json:"minority_interest"`
	NetCashDebt                            float64   `json:"net_cash_debt"`
	NetCashGrowth                          float64   `json:"net_cash_growth"`
	NetCashPerShare                        float64   `json:"net_cash_per_share"`
	OrderBacklog                           float64   `json:"order_backlog"`
	OtherCurrentAssets                     float64   `json:"other_current_assets"`
	OtherCurrentLiabilities                float64   `json:"other_current_liabilities"`
	OtherIntangibleAssets                  float64   `json:"other_intangible_assets"`
	OtherInvestments                       float64   `json:"other_investments"`
	OtherLongTermAssets                    float64   `json:"other_long_term_assets"`
	OtherLongTermLiabilities               float64   `json:"other_long_term_liabilities"`
	OtherReceivables                       float64   `json:"other_receivables"`
	PropertyPlantEquipment                 float64   `json:"property_plant_equipment"`
	PrepaidExpenses                        float64   `json:"prepaid_expenses"`
	PeriodEnding                           time.Time `json:"period_ending"`
	FiscalQuarter                          time.Time `json:"fiscal_quarter" db:"PrimaryKey"`
	Receivables                            float64   `json:"receivables"`
	RestrictedCash                         float64   `json:"restricted_cash"`
	RetainedEarnings                       float64   `json:"retained_earnings"`
	ShareholdersEquity                     float64   `json:"shareholders_equity"`
	Symbol                                 string    `json:"symbol" db:"PrimaryKey"`
	ShortTermDebt                          float64   `json:"short_term_debt"`
	ShortTermInvestments                   float64   `json:"short_term_investments"`
	TangibleBookValue                      float64   `json:"tangible_book_value"`
	TangibleBookValuePerShare              float64   `json:"tangible_book_value_per_share"`
	TotalAssets                            float64   `json:"total_assets"`
	TotalCommonEquity                      float64   `json:"total_common_equity"`
	TotalCommonSharesOutstanding           float64   `json:"total_common_shares_outstanding"`
	TotalCurrentAssets                     float64   `json:"total_current_assets"`
	TotalCurrentLiabilities                float64   `json:"total_current_liabilities"`
	TotalDebt                              float64   `json:"total_debt"`
	TotalInvestments                       float64   `json:"total_investments"`
	TotalLiabilities                       float64   `json:"total_liabilities"`
	TotalLiabilitiesEquity                 float64   `json:"total_liabilities_equity"`
	TotalLongTermAssets                    float64   `json:"total_long_term_assets"`
	TotalLongTermLiabilities               float64   `json:"total_long_term_liabilities"`
	TradingAssetSecurities                 float64   `json:"trading_asset_securities"`
	TreasuryStock                          float64   `json:"treasury_stock"`
	WorkingCapital                         float64   `json:"working_capital"`
	PolicyLoans                            float64   `json:"policy_loans"`
	ReinsuranceRecoverable                 float64   `json:"reinsurance_recoverable"`
	DeferredPolicyAcquisitionCost          float64   `json:"deferred_policy_acquisition_cost"`
	InsuranceAnnuityLiabilities            float64   `json:"insurance_annuity_liabilities"`
	UnpaidClaims                           float64   `json:"unpaid_claims"`
	DeferredLongTermCharges                float64   `json:"deferred_long_term_charges"`
	UnearnedPremiums                       float64   `json:"unearned_premiums"`
	ReinsurancePayable                     float64   `json:"reinsurance_payable"`
	InvestmentSecurities                   float64   `json:"investment_securities"`
	MortgageBackedSecurities               float64   `json:"mortgage_backed_securities"`
	DistributionsInExcessOfEarnings        float64   `json:"distributions_in_excess_of_earnings"`
	GrossLoans                             float64   `json:"gross_loans"`
	PreferredStockConvertible              float64   `json:"preferred_stock_convertible"`
	PreferredStockRedeemable               float64   `json:"preferred_stock_redeemable"`
	AllowanceForLoanLosses                 float64   `json:"allowance_for_loan_losses"`
	TotalPreferredEquity                   float64   `json:"total_preferred_equity"`
	OtherAdjustmentsToGrossLoans           float64   `json:"other_adjustments_to_gross_loans"`
	NetLoans                               float64   `json:"net_loans"`
	LoansHeldForSale                       float64   `json:"loans_held_for_sale"`
	AccruedInterestReceivable              float64   `json:"accrued_interest_receivable"`
	OtherRealEstateOwnedForeclosed         float64   `json:"other_real_estate_owned_foreclosed"`
	InterestBearingDeposits                float64   `json:"interest_bearing_deposits"`
	InstitutionalDeposits                  float64   `json:"institutional_deposits"`
	NonInterestBearingDeposits             float64   `json:"non_interest_bearing_deposits"`
	TotalDeposits                          float64   `json:"total_deposits"`
	ShortTermBorrowings                    float64   `json:"short_term_borrowings"`
	FederalHomeLongBankDebtLongTerm        float64   `json:"federal_home_loan_bank_debt_long_term"`
	TrustPreferredSecurities               float64   `json:"trust_preferred_securities"`
	PensionPostRetirementBenefits          float64   `json:"pension_post_retirement_benefits"`
	ComprehensiveIncomeOther               float64   `json:"comprehensive_income_other"`
	AccruedInterestPayable                 float64   `json:"accrued_interest_payable"`
	SeparateAccountAssets                  float64   `json:"separate_account_assets"`
	SeparateAccountLiability               float64   `json:"separate_account_liability"`
	LoansReceivableCurrent                 float64   `json:"loans_receivable_current"`
	RegulatoryAssets                       float64   `json:"regulatory_assets"`
	LongTermLoansReceivable                float64   `json:"long_term_loans_receivable"`
}

type FinancialsCashFlow struct {
	Acquisitions                                      float64   `json:"acquisitions"`
	AccruedInterestReceivable                         float64   `json:"accrued_interest_receivable"`
	AcquisitionOfRealEstateAssets                     float64   `json:"acquisition_of_real_estate_assets"`
	AssetWritedownRestructuringCosts                  float64   `json:"asset_writedown_restructuring_costs"`
	CaptialExpenditures                               float64   `json:"capital_expenditures"`
	CashAcquistions                                   float64   `json:"cash_acquisitions"`
	CashIncomeTaxPaid                                 float64   `json:"cash_income_tax_paid"`
	CashInterestPaid                                  float64   `json:"cash_interest_paid"`
	ChangeInAccountsReceivable                        float64   `json:"change_in_accounts_receivable"`
	ChangeInAccountsPayable                           float64   `json:"change_in_accounts_payable"`
	ChangeInDeferredTaxes                             float64   `json:"change_in_deferred_taxes"`
	ChangeInIncomeTaxes                               float64   `json:"change_in_income_taxes"`
	ChangeInInventory                                 float64   `json:"change_in_inventory"`
	ChangeInInvestments                               float64   `json:"change_in_investments"`
	ChangeInInsuranceReservesLiabilities              float64   `json:"change_in_insurance_reserves_liabilities"`
	ChangeInNetWorkingCapital                         float64   `json:"change_in_net_working_capital"`
	ChangeInOtherNetOperatingAssets                   float64   `json:"change_in_other_net_operating_assets"`
	ChangeInUnearnedRevenue                           float64   `json:"change_in_unearned_revenue"`
	CommonDividendsPaid                               float64   `json:"common_dividends_paid"`
	CommonStockIssued                                 float64   `json:"common_stock_issued"`
	CostOfRevenue                                     float64   `json:"cost_of_revenue"`
	DebtIssuedPaid                                    float64   `json:"debt_issued_paid"`
	DepreciationAmortization                          float64   `json:"depreciation_amortization"`
	DividendGrowth                                    float64   `json:"dividend_growth"`
	DividendPerShare                                  float64   `json:"dividend_per_share"`
	DividendsPaid                                     float64   `json:"dividends_paid"`
	Divestitures                                      float64   `json:"divestitures"`
	EBIT                                              float64   `json:"ebit"`
	EBITMargin                                        float64   `json:"ebit_margin"`
	EBITDA                                            float64   `json:"ebitda"`
	EBITDAMargin                                      float64   `json:"ebitda_margin"`
	EffectiveTaxRate                                  float64   `json:"effective_tax_ratsde"`
	EPSBasic                                          float64   `json:"eps_basic"`
	EPSDiluted                                        float64   `json:"eps_diluted"`
	EPSGrowth                                         float64   `json:"eps_growth"`
	ExchangeRateEffect                                float64   `json:"exchange_rate_effect"`
	FinancinCashFlow                                  float64   `json:"financing_cash_flow"`
	ForeignExchangeRateAdjustments                    float64   `json:"foreign_exchange_rate_adjustments"`
	FreeCashFlow                                      float64   `json:"free_cash_flow"`
	FreeCashFlowGrowth                                float64   `json:"free_cash_flow_growth"`
	FreeCashFlowMargin                                float64   `json:"free_cash_flow_margin"`
	FreeCashFlowPerShare                              float64   `json:"free_cash_flow_per_share"`
	GainLossOnSaleOfAssets                            float64   `json:"gain_loss_on_sale_of_assets"`
	GainLossOnSaleOfInvestments                       float64   `json:"gain_loss_on_sale_of_investments"`
	GainOnSaleOfLoansReceivables                      float64   `json:"gain_on_sale_of_loans_receivables"`
	GrossMargin                                       float64   `json:"gross_margin"`
	GrossProfit                                       float64   `json:"gross_profit"`
	IncomeTax                                         float64   `json:"income_tax"`
	IssuanceOfPreferredStock                          float64   `json:"issuance_of_preferred_stock"`
	InterestExpenseIncome                             float64   `json:"interest_expense_income"`
	InvestingCashFlow                                 float64   `json:"investing_cash_flow"`
	InvestmentInSecurities                            float64   `json:"investment_in_securities"`
	IssuanceOfCommonStock                             float64   `json:"issuance_of_common_stock"`
	LeveredFreeCashFlow                               float64   `json:"levered_free_cash_flow"`
	LongTermDebtIssued                                float64   `json:"long_term_debt_issued"`
	LongTermDebtRepaid                                float64   `json:"long_term_debt_repaid"`
	LossGainFromSaleOfAssets                          float64   `json:"loss_gain_from_sale_of_assets"`
	LossGainFromSaleOfInvestments                     float64   `json:"loss_gain_from_sale_of_investments"`
	LossGainOnEquityInvestments                       float64   `json:"loss_gain_on_equity_investments"`
	MiscellaneousCashFlowAdjustments                  float64   `json:"miscellaneous_cash_flow_adjustments"`
	NetCashFlow                                       float64   `json:"net_cash_flow"`
	NetDebtIssuedRepaid                               float64   `json:"net_debt_issued_repaid"`
	NetDecreaseIncreaseInLoansOriginatedSoldInvestin  float64   `json:"net_decrease_increase_in_loans_originated_sold_investing"`
	NetDecreaseIncreaseInLoansOriginatedSoldOperating float64   `json:"net_decrease_increase_in_loans_originated_sold_operating"`
	NetIncome                                         float64   `json:"net_income"`
	NetIncomeGrowth                                   float64   `json:"net_income_growth"`
	NetIncomeGrNetSaleAcqOfRealEstateAssets           float64   `json:"net_sale_acq_of_real_estate_assets"`
	OperatingCashFlow                                 float64   `json:"operating_cash_flow"`
	OperatingCashFlowGrowth                           float64   `json:"operating_cash_flow_growth"`
	OperatingExpenses                                 float64   `json:"operating_expenses"`
	OperatingIncome                                   float64   `json:"operating_income"`
	OperatingMargin                                   float64   `json:"operating_margin"`
	OtherAmortization                                 float64   `json:"other_amortization"`
	OtherExpenseIncome                                float64   `json:"other_expense_income"`
	OtherFinancingActivities                          float64   `json:"other_financing_activities"`
	OtherInvestinActivities                           float64   `json:"other_investing_activities"`
	OtherOperatingActivities                          float64   `json:"other_operating_activities"`
	OtherOperatingExpenses                            float64   `json:"other_operating_expenses"`
	PreferredDividendsPaid                            float64   `json:"preferred_dividends_paid"`
	PretaxIncome                                      float64   `json:"pretax_income"`
	ProfitMargin                                      float64   `json:"profit_margin"`
	ProvisionForCreditLosses                          float64   `json:"provision_for_credit_losses"`
	ProvisionWriteOffOfBadDebts                       float64   `json:"provision_write_off_of_bad_debts"`
	PeriodEnding                                      time.Time `json:"period_ending"`
	FiscalQuarter                                     time.Time `json:"fiscal_quarter" db:"PrimaryKey"`
	ReinsuranceRecoverable                            float64   `json:"reinsurance_recoverable"`
	RepurchasesOfCommonStock                          float64   `json:"repurchases_of_common_stock"`
	RepurchaseOfCommonStock                           float64   `json:"repurchase_of_common_stock"`
	RepurchasesOfPreferredStock                       float64   `json:"repurchases_of_preferred_stock"`
	ResearchDevelopment                               float64   `json:"research_development"`
	Revenue                                           float64   `json:"revenue"`
	RevenueGrowthYOY                                  float64   `json:"revenue_growth_yoy"`
	SaleOfPropertyPlantEquipment                      float64   `json:"sale_of_property_plant_equipment"`
	SalePurchaseOfIntangibles                         float64   `json:"sale_purchase_of_intangibles"`
	SellingGeneralAdmin                               float64   `json:"selling_general_admin"`
	ShareBasedCompensation                            float64   `json:"share_based_compensation"`
	ShareIssuanceRepurchase                           float64   `json:"share_issuance_repurchase"`
	SharesChange                                      float64   `json:"shares_change"`
	SharesOutstandingBasic                            float64   `json:"shares_outstanding_basic"`
	SharesOutstandingDiluted                          float64   `json:"shares_outstanding_diluted"`
	ShareRepurchases                                  float64   `json:"share_repurchases"`
	ShortTermDebtIssued                               float64   `json:"short_term_debt_issued"`
	ShortTermDebtRepaid                               float64   `json:"short_term_debt_repaid"`
	StockBasedCompensation                            float64   `json:"stock_based_compensation"`
	Symbol                                            string    `json:"symbol" db:"PrimaryKey"`
	TotalAssetWritedown                               float64   `json:"total_asset_writedown"`
	TotalDebtIssued                                   float64   `json:"total_debt_issued"`
	TotalDebtRepaid                                   float64   `json:"total_debt_repaid"`
	TotalDividendsPaid                                float64   `json:"total_dividends_paid"`
	UnleveredFreeCashFlow                             float64   `json:"unlevered_free_cash_flow"`
	SaleOfPropertyPlantAndEquipment                   float64   `json:"sale_of_property_plant_and_equipment"`
	InvestmentInMarketableEquitySecurities            float64   `json:"investment_in_marketable_equity_securities"`
	PreferredShareRepurchases                         float64   `json:"preferred_share_repurchases"`
	PurchaseSaleOfIntangibles                         float64   `json:"purchase_sale_of_intangibles"`
	DeferredPolicyAcquisitionCost                     float64   `json:"deferred_policy_acquisition_cost"`
	NetIncreaseDecreaseInDepositAccounts              float64   `json:"net_increase_decrease_in_deposit_accounts"`
	AssetWritedown                                    float64   `json:"asset_writedown"`
	LossGainOnSaleOfAssets                            float64   `json:"loss_gain_on_sale_of_assets"`
	NuclearFuelExpenditures                           float64   `json:"nuclear_fuel_expenditures"`
	PurchaseSaleOfIntangibleAssets                    float64   `json:"purchase_sale_of_intangible_assets"`
	ContributionsToNuclearDemissioningTrust           float64   `json:"contributions_to_nuclear_demissioning_trust"`
	LossGainOnSaleOfInvestments                       float64   `json:"loss_gain_on_sale_of_investments"`
	SaleOfRealEstateAssets                            float64   `json:"sale_of_real_estate_assets"`
	PreferredStockIssued                              float64   `json:"preferred_stock_issued"`
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
	InventoryTurnover      float64   `json:"inventory_turnover"`
	LastClosePrice         float64   `json:"last_close_price"`
	MarketCapGrowth        float64   `json:"market_cap_growth"`
	MarketCapitalization   float64   `json:"market_capitalization"`
	PFCFRatio              float64   `json:"p_fcf_ratio"`
	POCFRatio              float64   `json:"p_ocf_ratio"`
	PayoutRatio            float64   `json:"payout_ratio"`
	PBRatio                float64   `json:"pb_ratio"`
	PERatio                float64   `json:"pe_ratio"`
	PriceAffoRatio         float64   `json:"price_affo_ratio"`
	PriceFfoRatio          float64   `json:"price_ffo_ratio"`
	PSRatio                float64   `json:"ps_ratio"`
	PeriodEnding           time.Time `json:"period_ending"`
	FiscalQuarter          time.Time `json:"fiscal_quarter" db:"PrimaryKey"`
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
