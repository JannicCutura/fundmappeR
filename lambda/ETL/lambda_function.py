import re
import os
import xml.etree.ElementTree as ET
import pandas as pd
import boto3
import csv
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

from xml_2_data import mnfp_2_data
from xml_2_data import mnfp1_2_data
from xml_2_data import mnfp2_2_data
from nmfp_rename_vars import nmfp_rename_vars


def lambda_handler(event, context):
    # parse the S3 triggered event
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    # bucket = "fundmapper"
    # key = "02-RawNMFPs/S000000623/2011-01-07-S000000623.txt"

    prefix, series_id, filing = key.split("/")
    print(bucket)
    print(series_id)
    print(filing)
    # store temporarily
    s3_client.download_file(bucket, key, "/tmp/" + series_id + "_" + filing + ".txt")
    s3.Object(bucket, key).delete()
    # read
    filing = open("/tmp/" + series_id + "_" + filing + ".txt", 'r').read()
    filing = filing.replace(":", "")
    filing_type = re.search("<TYPE>(.*)\n", filing).group(1)
    filing_date = int(re.sub("[^0-9]", "", re.search("CONFORMED PERIOD OF REPORT(.*)\n", filing).group(1))[0:6])
    filing_year = int(re.sub("[^0-9]", "", re.search("CONFORMED PERIOD OF REPORT(.*)\n", filing).group(1))[0:4])
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

    # add series id
    series_df['series_id'], class_df['series_id'], holdings['series_id'], all_collateral[
        'series_id'] = series_id, series_id, series_id, series_id

    # holdings
    holdings_str_columns = ['filing_type', 'repurchaseAgreement', 'securityDemandFeatureFlag',
                            'guarantorList', 'InvestmentIdentifier', 'NRSRO',
                            'isFundTreatingAsAcquisitionUnderlyingSecurities',
                            'finalLegalInvestmentMaturityDate', 'cik', 'weeklyLiquidAssetSecurityFlag', 'rating',
                            'investmentCategory', 'repurchaseAgreementList', 'dailyLiquidAssetSecurityFlag',
                            'securityCategorizedAtLevel3Flag', 'CUSIPMember', 'investmentMaturityDateWAM',
                            'ISINId', 'LEIID', 'titleOfIssuer', 'securityEnhancementsFlag', 'InvestmentTypeDomain',
                            'securityGuaranteeFlag', 'fundAcqstnUndrlyngSecurityFlag',
                            'securityEligibilityFlag', 'otherUniqueId', 'demandFeatureIssuerList', 'nameOfIssuer',
                            'illiquidSecurityFlag', 'series_id']
    holdings_float_columns = ['yieldOfTheSecurityAsOfReportingDate', 'investmentMaturityDateWAL',
                              'AvailableForSaleSecuritiesAmortizedCost',
                              'includingValueOfAnySponsorSupport', 'excludingValueOfAnySponsorSupport',
                              'InvestmentOwnedBalancePrincipalAmount', 'percentageOfMoneyMarketFundNetAssets', ]
    holdings_int_columns = ['date', 'issuer_number']
    holdings_columns = holdings_str_columns + holdings_float_columns + holdings_int_columns

    holdings_data = pd.DataFrame(columns=holdings_columns)
    holdings_data = holdings_data.append(holdings)
    del holdings

    # collateral
    collateral_str_columns = ['ctgryInvestmentsRprsntsCollateral', 'filing_type', 'LEIID',
                              'principalAmountToTheNearestCent',
                              'maturityDate', 'series_id', 'nameOfCollateralIssuer']
    collateral_int_columns = ['issuer_number', 'date']
    collateral_float_columns = ['couponOrYield', 'valueOfCollateralToTheNearestCent',
                                'AssetsSoldUnderAgreementsToRepurchaseCarryingAmounts',
                                'CashCollateralForBorrowedSecurities']
    collateral_columns = collateral_str_columns + collateral_int_columns + collateral_float_columns

    collateral_data = pd.DataFrame(columns=collateral_columns)
    collateral_data = collateral_data.append(all_collateral)
    del all_collateral

    # series
    series_str_columns = ['subAdviserList', 'filing_type', 'fundExemptRetailFlag', 'ContainedFileInformationFileNumber',
                          'transferAgent', 'adviser', 'investmentAdviserList',
                          'dateCalculatedFornetValuePerShareIncludingCapitalSupportAgreement', 'transferAgentList',
                          'masterFundFlag', 'seriesFundInsuCmpnySepAccntFlag',
                          'dateCalculatedFornetValuePerShareExcludingCapitalSupportAgreement', 'feederFundFlag',
                          'InvestmentTypeDomain', 'administratorList', 'series_id', 'subAdviser', 'indpPubAccountant']
    series_float_columns = ['totalValueDailyLiquidAssets_fridayDay3', 'averageLifeMaturity',
                            'totalValueWeeklyLiquidAssets_fridayWeek2', 'percentageDailyLiquidAssets_fridayDay4',
                            'percentageWeeklyLiquidAssets_fridayWeek1', 'independentPublicAccountant',
                            'totalValueDailyLiquidAssets_fridayDay1', 'percentageWeeklyLiquidAssets_fridayWeek2',
                            'netAssetOfSeries', 'averagePortfolioMaturity', 'totalValueOtherAssets',
                            'AvailableForSaleSecuritiesAmortizedCost',
                            'netValuePerShareIncludingCapitalSupportAgreement', 'moneyMarketFundCategory',
                            'totalValueDailyLiquidAssets_fridayDay4', 'sevenDayGrossYield', 'securitiesActFileNumber',
                            'numberOfSharesOutstanding', 'netAssetValue_fridayWeek3',
                            'percentageDailyLiquidAssets_fridayDay3', 'percentageDailyLiquidAssets_fridayDay2',
                            'netValuePerShareExcludingCapitalSupportAgreement', 'totalValueLiabilities',
                            'netAssetValue_fridayWeek2', 'percentageWeeklyLiquidAssets_fridayWeek3',
                            'totalValuePortfolioSecurities', 'totalValueWeeklyLiquidAssets_fridayWeek3',
                            'percentageWeeklyLiquidAssets_fridayWeek4', 'totalValueWeeklyLiquidAssets_fridayWeek4',
                            'stablePricePerShare', 'cash', 'netAssetValue_fridayWeek4',
                            'amortizedCostPortfolioSecurities', 'totalValueDailyLiquidAssets_fridayDay2',
                            'netAssetValue_fridayWeek1', 'totalValueWeeklyLiquidAssets_fridayWeek1',
                            'percentageDailyLiquidAssets_fridayDay1']
    series_int_columns = ['date']
    series_columns = series_str_columns + series_float_columns + series_int_columns

    series_data = pd.DataFrame(columns=series_columns)
    series_data = series_data.append(series_df)
    del series_df

    class_str_columns = ['series_id', 'classesId', 'personPayForFundFlag', 'filing_type', 'nameOfPersonDescExpensePay']
    class_int_columns = ['date']
    class_float_columns = ['fridayWeek1_weeklyGrossRedemptions', 'totalForTheMonthReported_weeklyGrossSubscriptions',
                           'fridayWeek4_weeklyGrossRedemptions', 'netShareholderFlowActivityForMonthEnded',
                           'minInitialInvestment', 'totalForTheMonthReported_weeklyGrossRedemptions',
                           'netAssetValuePerShareIncludingCapitalSupportAgreement', 'netAssetsOfClass',
                           'fridayWeek1_weeklyGrossSubscriptions', 'fridayWeek2_weeklyGrossRedemptions',
                           'fridayWeek3_weeklyGrossRedemptions', 'netAssetPerShare', 'numberOfSharesOutstanding',
                           'netAssetValuePerShareExcludingCapitalSupportAgreement',
                           'fridayWeek4_weeklyGrossSubscriptions', 'fridayWeek3_weeklyGrossSubscriptions',
                           'fridayWeek2_weeklyGrossSubscriptions', 'sevenDayNetYield']
    class_columns = class_str_columns + class_int_columns + class_float_columns

    class_data = pd.DataFrame(columns=class_columns)
    class_data = class_data.append(class_df)
    del class_df

    ## convert data types
    # class data
    class_data[class_str_columns] = class_data[class_str_columns].astype("string")
    class_data[class_int_columns] = class_data[class_int_columns].astype(int)
    class_data[class_float_columns] = class_data[class_float_columns].apply(pd.to_numeric, errors="coerce")

    # series data
    series_data[series_str_columns] = series_data[series_str_columns].astype("string")
    series_data[series_float_columns] = series_data[series_float_columns].apply(pd.to_numeric, errors="coerce")
    series_data[series_int_columns] = series_data[series_int_columns].astype(int)

    # holdings
    holdings_data[holdings_str_columns] = holdings_data[holdings_str_columns].astype("string")
    holdings_data[holdings_int_columns] = holdings_data[holdings_int_columns].astype(int)
    holdings_data[holdings_float_columns] = holdings_data[holdings_float_columns].apply(pd.to_numeric, errors="coerce")

    # collateral
    collateral_data[collateral_str_columns] = collateral_data[collateral_str_columns].astype("string")
    collateral_data[collateral_int_columns] = collateral_data[collateral_int_columns].astype(int)
    collateral_data[collateral_float_columns] = collateral_data[collateral_float_columns].apply(pd.to_numeric,
                                                                                                errors="coerce")

    # order
    class_data = class_data[class_columns]
    series_data = series_data[series_columns]
    collateral_data = collateral_data[collateral_columns]
    holdings_data = holdings_data[holdings_columns]

    # add variables, i.e. colaece

    collateral_data["CollateralValue"] = collateral_data['CashCollateralForBorrowedSecurities'].combine_first(
        collateral_data['valueOfCollateralToTheNearestCent'])
    holdings_data["Type"] = holdings_data.InvestmentTypeDomain.combine_first(holdings_data.investmentCategory)

    file_format = ".csv"
    header = True

    if series_data.shape[0] >= 1:
        series_data.to_csv("/tmp/series_" + series_id + "_" + str(filing_date) + file_format, index=False,
                           header=header)
        s3_client.upload_file("/tmp/series_" + series_id + "_" + str(filing_date) + file_format, "fundmapper",
                              "03-ParsedRecords/series_data/" + str(filing_year) + "/" + series_id + "_" + str(
                                  filing_date) + file_format)

    if class_data.shape[0] >= 1:
        class_data.to_csv("/tmp/class_" + series_id + "_" + str(filing_date) + file_format, index=False, header=header)
        s3_client.upload_file("/tmp/class_" + series_id + "_" + str(filing_date) + file_format, "fundmapper",
                              "03-ParsedRecords/class_data/" + str(filing_year) + "/" + series_id + "_" + str(
                                  filing_date) + file_format)

    if holdings_data.shape[0] >= 1:
        holdings_data.to_csv("/tmp/holdings_" + series_id + "_" + str(filing_date) + file_format, index=False,
                             header=header)
        s3_client.upload_file("/tmp/holdings_" + series_id + "_" + str(filing_date) + file_format, "fundmapper",
                              "03-ParsedRecords/holdings_data/" + str(filing_year) + "/" + series_id + "_" + str(
                                  filing_date) + file_format)

    if collateral_data.shape[0] >= 1:
        collateral_data.to_csv("/tmp/collateral_" + series_id + "_" + str(filing_date) + file_format, index=False,
                               header=header)
        s3_client.upload_file(f"/tmp/collateral_{series_id}_{filing_date}{file_format}", "fundmapper",
                              f"03-ParsedRecords/collateral_data/{filing_year}/{series_id}_{filing_date}{file_format}")

    return "Success"