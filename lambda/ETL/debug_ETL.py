import boto3
s3 = boto3.client('s3')
import codecs
import csv
import pandas as pd
result = s3.get_bucket_acl(Bucket='fundmapper')
print(result)

filename = "2011-02-04-S000008702.txt"
s3.download_file('fundmapper', f'02-RawNMFPs/S000008702/{filename}', f'{filename}')

filing = open(f"{filename}", 'r').read()
filing = filing.replace(":", "")
filing_type = re.search("<TYPE>(.*)\n", filing).group(1)
filing_date = int(re.sub("[^0-9]", "", re.search("CONFORMED PERIOD OF REPORT(.*)\n", filing).group(1))[0:6])
filing = (filing.replace("\n", "")
          .replace(' xmlns="http//www.sec.gov/edgar/nmfpsecurities"', '')
          .replace(' xmlns="http//www.sec.gov/edgar/nmfpfund"', ""))
print("convert")
if filing_type in ["N-MFP", "N-MFP/A"]:
    series_df, class_df, holdings, all_collateral = mnfp_2_data(filing)
    series_df, class_df, holdings, all_collateral = nmfp_rename_vars(filing_type, series_df, class_df, holdings,
                                                                     all_collateral)

if filing_type in ["N-MFP1", "N-MFP1/A"]:
    series_df, class_df, holdings, all_collateral = mnfp1_2_data(filing)

if filing_type in ["N-MFP2", "N-MFP2/A"]:
    series_df, class_df, holdings, all_collateral = mnfp2_2_data(filing)


# drop , from all fields, GLUE doesn't get it seems...
series_df.replace({",": " "}, regex=True, inplace=True)
class_df.replace({",": " "}, regex=True, inplace=True)
holdings.replace({",": " "}, regex=True, inplace=True)
all_collateral.replace({",": " "}, regex=True, inplace=True)

# add date
series_df['date'], class_df['date'], holdings['date'], all_collateral[
    'date'] = filing_date, filing_date, filing_date, filing_date

# add filing type
series_df['filing_type'], class_df['filing_type'], holdings['filing_type'], all_collateral[
    'filing_type'] = filing_type, filing_type, filing_type, filing_type,

## convert data types
# class data
class_df[['minInitialInvestment', 'netAssetsOfClass',
          'netAssetPerShare', 'netShareholderFlowActivityForMonthEnded',
          'totalForTheMonthReported_weeklyGrossSubscriptions',
          'totalForTheMonthReported_weeklyGrossRedemptions', 'sevenDayNetYield',
          'netAssetValuePerShareIncludingCapitalSupportAgreement',
          'netAssetValuePerShareExcludingCapitalSupportAgreement']] = class_df[['minInitialInvestment', 'netAssetsOfClass',
'netAssetPerShare', 'netShareholderFlowActivityForMonthEnded',
'totalForTheMonthReported_weeklyGrossSubscriptions',
'totalForTheMonthReported_weeklyGrossRedemptions', 'sevenDayNetYield',
'netAssetValuePerShareIncludingCapitalSupportAgreement',
'netAssetValuePerShareExcludingCapitalSupportAgreement']].apply(pd.to_numeric, errors="coerce")
class_df[["filing_type", 'classesId']] = class_df[["filing_type", 'classesId']].astype("string")

# series data
series_df[['ContainedFileInformationFileNumber', 'investmentAdviserList',
       'subAdviserList', 'independentPublicAccountant', 'administratorList',
       'transferAgentList', 'feederFundFlag', 'masterFundFlag',
       'seriesFundInsuCmpnySepAccntFlag', 'InvestmentTypeDomain',
       'averagePortfolioMaturity', 'averageLifeMaturity', '',
       'totalValueOtherAssets', 'totalValueLiabilities', 'netAssetOfSeries',
       'sevenDayGrossYield',
       'netValuePerShareIncludingCapitalSupportAgreement',
       'dateCalculatedFornetValuePerShareIncludingCapitalSupportAgreement',
       'netValuePerShareExcludingCapitalSupportAgreement',
       'dateCalculatedFornetValuePerShareExcludingCapitalSupportAgreement',
       'date', 'filing_type']]

# reorder columns

holdings_data = pd.DataFrame(columns=["filing_type", "date", "issuer_number", "nameOfIssuer", "titleOfIssuer",
                                      "InvestmentIdentifier", "cik", "InvestmentTypeDomain", "investmentCategory",
                                      "isFundTreatingAsAcquisitionUnderlyingSecurities",
                                      "repurchaseAgreementList", "rating", "investmentMaturityDateWAM",
                                      "finalLegalInvestmentMaturityDate", "securityDemandFeatureFlag",
                                      "securityGuaranteeFlag", "securityEnhancementsFlag",
                                      "InvestmentOwnedBalancePrincipalAmount",
                                      "AvailableForSaleSecuritiesAmortizedCost",
                                      "percentageOfMoneyMarketFundNetAssets", "illiquidSecurityFlag",
                                      "includingValueOfAnySponsorSupport", "excludingValueOfAnySponsorSupport",
                                      "CUSIPMember", "guarantorList", "demandFeatureIssuerList"])
holdings_data = holdings_data.append(holdings)
del holdings

collateral_data = pd.DataFrame(
    columns=['filing_type', 'date', 'issuer_number', 'nameOfCollateralIssuer', 'LEIID', 'maturityDate',
             'couponOrYield',
             'principalAmountToTheNearestCent', 'valueOfCollateralToTheNearestCent',
             'ctgryInvestmentsRprsntsCollateral'])
collateral_data = collateral_data.append(all_collateral)
del all_collateral















