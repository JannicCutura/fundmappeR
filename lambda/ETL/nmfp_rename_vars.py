def nmfp_rename_vars(filing_type, series_df, class_df, holdings, all_collateral):
    '''
    Returns inputs with renamed columns for N-MFP type documents

            Parameters:
                    filing_type (str): the filing type
                    series_df (pandas): a dataframe with series information
                    class_df (pandas): a dataframe with class information
                    holdings (pandas): a dataframe with holdings
                    all_collateral (pandas): a dataframe with collateral information

            Returns:
                    binary_sum (str): Binary string of the sum of a and b
    '''
    if filing_type in ["N-MFP", "N-MFP/A"]:
        series_df.rename(
            columns={'isThisFeederFund': "feederFundFlag",
                     'isThisMasterFund': 'masterFundFlag',
                     'isThisSeriesPrimarilyUsedToFundInsuranceCompanySeperateAccounts': 'seriesFundInsuCmpnySepAccntFlag',
                     'moneyMarketFundCategory': 'InvestmentTypeDomain',
                     'dollarWeightedAveragePortfolioMaturity': 'averagePortfolioMaturity',
                     'dollarWeightedAverageLifeMaturity': 'averageLifeMaturity',
                     'AvailableForSaleSecuritiesAmortizedCost': 'AvailableForSaleSecuritiesAmortizedCost',
                     'OtherAssets': 'totalValueOtherAssets',
                     'Liabilities': 'totalValueLiabilities',
                     'AssetsNet': 'netAssetOfSeries',
                     'MoneyMarketSevenDayYield': 'sevenDayGrossYield'
                     }, inplace=True)

        class_df.rename(
            columns={'classId': 'classesId',
                     'minInitialInvestment': 'minInitialInvestment',
                     'netAssetsOfClass': 'netAssetsOfClass',
                     'netAssetValuePerShare': 'netAssetPerShare',
                     'sevenDayNetYield': 'sevenDayNetYield',
                     'grossSubscriptionsForMonthEnded': 'totalForTheMonthReported_weeklyGrossSubscriptions',
                     'grossRedemptionsForMonthEnded': 'totalForTheMonthReported_weeklyGrossRedemptions'},
            inplace=True)

        holdings.rename(
            columns={'AvailableForSaleSecuritiesAmortizedCost': 'AvailableForSaleSecuritiesAmortizedCost',
                     'CUSIPMember': 'CUSIPMember',
                     'EntityCentralIndexKey': 'cik',
                     'isThisIlliquidSecurity': 'illiquidSecurityFlag',
                     'issuer_number': 'issuer_number',
                     'categoryOfInvestmentDesc': 'investmentCategory',
                     'finalLegalInvestmentMaturityDate': 'finalLegalInvestmentMaturityDate',
                     'InvestmentOwnedPercentOfNetAssets': 'percentageOfMoneyMarketFundNetAssets',
                     'doesSecurityHaveGuarantee': 'securityGuaranteeFlag',
                     'doesSecurityHaveDemandFeature': 'securityDemandFeatureFlag',
                     'doesSecurityHaveEnhancementsOnWhichFundRelying': 'securityEnhancementsFlag',
                     'InvestmentIssuer': 'nameOfIssuer',
                     'InvestmentTitle': 'titleOfIssuer',
                     'InvestmentTypeDomain': 'InvestmentTypeDomain',
                     'InvestmentMaturityDate': 'investmentMaturityDateWAM',
                     'valueOfSecurityExcludingValueOfCapitalSupportAgreement': 'excludingValueOfAnySponsorSupport',
                     "InvestmentOwnedAtFairValue": 'includingValueOfAnySponsorSupport'},
            inplace=True)

        all_collateral.rename(
            columns={'issuer_number': 'issuer_number',
                     'InvestmentIssuer': 'nameOfCollateralIssuer',
                     'InvestmentMaturityDate': 'maturityDate',
                     'CR': 'couponOrYield',
                     'InvestmentTypeDomain': 'ctgryInvestmentsRprsntsCollateral'},
            inplace=True)

    if 'enhancementsList' in holdings.columns:
        holdings.drop(['enhancementsList'], axis=1, inplace=True)

    return series_df, class_df, holdings, all_collateral
