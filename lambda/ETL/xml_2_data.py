import re
import os
import xml.etree.ElementTree as ET
import pandas as pd

def mnfp_2_data(filing):
    ## series level
    seriesLevelInformation_start = filing.find("<seriesLevelInformation>")
    seriesLevelInformation_end = filing.find("</seriesLevelInformation>")
    seriesLevelInformation = filing[seriesLevelInformation_start:seriesLevelInformation_end + 25]
    seriesLevelInformation = seriesLevelInformation.replace(" xmlns=\"http//www.sec.gov/edgar/nmfpfund\"", "")
    seriesLevelInformation = seriesLevelInformation.replace("part1", "")
    seriesLevelInformation = seriesLevelInformation.replace("<seriesShadowPrice>", "")
    seriesLevelInformation = seriesLevelInformation.replace("</seriesShadowPrice>", "")
    seriesLevelInformation = seriesLevelInformation.replace("\n", "")

    def mnfp_2_series(xml_data):
        root = ET.XML(xml_data)  # element tree
        series_df = {}

        for item_number, item in enumerate(root):
            series_df[item.tag] = item.text

        series_df = pd.DataFrame(series_df, index=[0])
        return series_df

    series_df = mnfp_2_series(seriesLevelInformation)

    # class level
    classLevelInformation_start = filing.find("<classLevelInformationList>")
    classLevelInformation_end = filing.find("</classLevelInformationList>")
    classLevelInformation = filing[
                            classLevelInformation_start:classLevelInformation_end + len("</classLevelInformationList>")]
    classLevelInformation = classLevelInformation.replace("part1", "")
    classLevelInformation = classLevelInformation.replace("<classShadowPrice>", "")
    classLevelInformation = classLevelInformation.replace("</classShadowPrice>", "")
    classLevelInformation = classLevelInformation.replace("\n", "")


    def mnfp_2_class(xml_data):
        root = ET.XML(xml_data)  # element tree
        class_df = pd.DataFrame()

        for share_number, shareclass in enumerate(root):
            class_dict = {}
            for item in shareclass:
                class_dict[item.tag] = item.text
            class_df = class_df.append(pd.DataFrame(class_dict, index=[0]))

        return class_df

    class_df = mnfp_2_class(classLevelInformation)

    # collateral and holdings
    scheduleOfPortfolioSecuritiesList_start = filing.find("<scheduleOfPortfolioSecuritiesList>")
    scheduleOfPortfolioSecuritiesList_end = filing.find("</scheduleOfPortfolioSecuritiesList>") + 36
    scheduleOfPortfolioSecuritiesList = filing[scheduleOfPortfolioSecuritiesList_start:scheduleOfPortfolioSecuritiesList_end]

    scheduleOfPortfolioSecuritiesList = scheduleOfPortfolioSecuritiesList.replace("part2", "")
    scheduleOfPortfolioSecuritiesList = scheduleOfPortfolioSecuritiesList.replace("<investdate>", "")
    scheduleOfPortfolioSecuritiesList = scheduleOfPortfolioSecuritiesList.replace("</investdate>", "")
    scheduleOfPortfolioSecuritiesList = scheduleOfPortfolioSecuritiesList.replace("\n", "")
    scheduleOfPortfolioSecuritiesList = re.sub("> +", ">", scheduleOfPortfolioSecuritiesList)
    scheduleOfPortfolioSecuritiesList = re.sub(" +<", "<", scheduleOfPortfolioSecuritiesList)
    xml_data = scheduleOfPortfolioSecuritiesList

    def mnfp_2_holdingscollateral(xml_data):
        root = ET.XML(xml_data)  # element tree
        all_holdings_records = []
        all_collateral_pd = pd.DataFrame()
        for issue_number, scheduleOfPortfolioSecurities in enumerate(root):
            holdings_record = {}

            for security_item in scheduleOfPortfolioSecurities:
                # grab all common fields (for repos, cp, cds...)
                holdings_record["issuer_number"] = issue_number + 1
                holdings_record[security_item.tag] = security_item.text
                all_collateral_records = pd.DataFrame()
                if security_item.tag == "repurchaseAgreementList":
                    for RepurchaseAgreement in security_item:
                        collateral_record = {}
                        for collateral in RepurchaseAgreement:
                            collateral_record["issuer_number"] = issue_number + 1
                            collateral_record[collateral.tag] = collateral.text

                        all_collateral_records = all_collateral_records.append(
                            pd.DataFrame(collateral_record, index=[0]))
                    all_collateral_pd = pd.concat([all_collateral_pd, all_collateral_records])

            all_holdings_records.append(holdings_record)
        holdings = pd.DataFrame(all_holdings_records)

        return holdings, all_collateral_pd

    holdings, all_collateral = mnfp_2_holdingscollateral(scheduleOfPortfolioSecuritiesList)

    return series_df, class_df, holdings, all_collateral

def mnfp1_2_data(filing):
    # series level
    seriesLevelInformation_start = filing.find("<seriesLevelInfo>")
    seriesLevelInformation_end = filing.find("</seriesLevelInfo>")
    seriesLevelInformation = filing[seriesLevelInformation_start:seriesLevelInformation_end + len("</seriesLevelInfo>")]
    seriesLevelInformation = seriesLevelInformation.replace(" xmlns=\"http//www.sec.gov/edgar/nmfpfund\"", "")
    seriesLevelInformation = seriesLevelInformation.replace("nmfp1common", "")
    seriesLevelInformation = seriesLevelInformation.replace("ns3", "")


    def mnfp1_2_series(xml_data):
        root = ET.XML(xml_data)
        series_df = {}

        for item_number, item in enumerate(root):
            if item.tag in ["totalValueDailyLiquidAssets", "totalValueWeeklyLiquidAssets","percentageDailyLiquidAssets",
                            "percentageWeeklyLiquidAssets","netAssetValue"]:
                for subitem in item:
                    series_df[item.tag +"_"+subitem.tag] = subitem.text
            else:
                series_df[item.tag] = item.text
        series_df = pd.DataFrame(series_df, index = [0])
        return series_df

    series_df = mnfp1_2_series(seriesLevelInformation)


    # class level
    classLevelInformation_start = filing.find("<classLevelInfo>")
    classLevelInformation_end = filing.find("</classLevelInfo>")
    classLevelInformation = filing[
                            classLevelInformation_start:classLevelInformation_end + len("</classLevelInfo>")]
    classLevelInformation = classLevelInformation.replace("ns3", "")
    classLevelInformation = classLevelInformation.replace("nmfp1common", "")
    xml_data = classLevelInformation

    def mnfp1_2_class(xml_data):
        root = ET.XML(xml_data)
        class_df = {}

        for item_number, item in enumerate(root):
            if item.tag in ["netAssetShare","fridayWeek1","fridayWeek2","fridayWeek3","fridayWeek4","fridayWeek5",
                            "totalForTheMonthReported"]:
                for subitem in item:
                    class_df[item.tag+"_"+subitem.tag] = subitem.text
            else:
                class_df[item.tag] = item.text

        class_df = pd.DataFrame(class_df, index = [0])
        return class_df

    class_df = mnfp1_2_class(classLevelInformation)

    # collateral and holdings
    scheduleOfPortfolioSecuritiesList_start = filing.find("<scheduleOfPortfolioSecuritiesInfo>")
    scheduleOfPortfolioSecuritiesList_end = filing.rfind("</scheduleOfPortfolioSecuritiesInfo>") + len("</scheduleOfPortfolioSecuritiesInfo>")
    scheduleOfPortfolioSecuritiesList = filing[scheduleOfPortfolioSecuritiesList_start:scheduleOfPortfolioSecuritiesList_end]
    scheduleOfPortfolioSecuritiesList = "<scheduleOfPortfolioSecuritiesList>" + scheduleOfPortfolioSecuritiesList  + "</scheduleOfPortfolioSecuritiesList>"
    scheduleOfPortfolioSecuritiesList = scheduleOfPortfolioSecuritiesList.replace("<date>", "")
    scheduleOfPortfolioSecuritiesList = scheduleOfPortfolioSecuritiesList.replace("</date>", "")
    scheduleOfPortfolioSecuritiesList = re.sub("> +", ">", scheduleOfPortfolioSecuritiesList)
    scheduleOfPortfolioSecuritiesList = re.sub(" +<", "<", scheduleOfPortfolioSecuritiesList)
    xml_data = scheduleOfPortfolioSecuritiesList

    def mnfp1_2_holdingscollateral(xml_data):
        root = ET.XML(xml_data)  # element tree
        all_holdings_records = []
        all_collateral_pd = pd.DataFrame()
        for issue_number, scheduleOfPortfolioSecurities in enumerate(root):
            holdings_record = {}

            for security_item in scheduleOfPortfolioSecurities:
                # grab all common fields (for repos, cp, cds...)
                holdings_record["issuer_number"] = issue_number + 1
                holdings_record[security_item.tag] = security_item.text
                all_collateral_records = pd.DataFrame()
                if security_item.tag == "repurchaseAgreement":
                    for RepurchaseAgreement in security_item:
                        #if RepurchaseAgreement.tag == "repurchaseAgreementOpenFlag":
                        #   for repurchaseAgreementOpenFlag in RepurchaseAgreement:
                        if RepurchaseAgreement.tag == "collateralIssuers":
                            collateral_record = {}
                            for collateral in RepurchaseAgreement:
                                collateral_record["issuer_number"] = issue_number + 1
                                collateral_record[collateral.tag] = collateral.text
                            all_collateral_pd = all_collateral_pd.append(pd.DataFrame(collateral_record, index=[0]))

            all_holdings_records.append(holdings_record)
        holdings = pd.DataFrame(all_holdings_records)

        return holdings, all_collateral_pd

    holdings, all_collateral = mnfp1_2_holdingscollateral(scheduleOfPortfolioSecuritiesList)

    return series_df, class_df, holdings, all_collateral

def mnfp2_2_data(filing):
    ## series level
    seriesLevelInformation_start = filing.find("<seriesLevelInfo>")
    seriesLevelInformation_end = filing.find("</seriesLevelInfo>")
    seriesLevelInformation = filing[seriesLevelInformation_start:seriesLevelInformation_end + len("</seriesLevelInfo>")]
    seriesLevelInformation = seriesLevelInformation.replace(" xmlns=\"http//www.sec.gov/edgar/nmfpfund\"", "")
    seriesLevelInformation = seriesLevelInformation.replace("nmfp2common", "")
    seriesLevelInformation = seriesLevelInformation.replace("ns3", "")

    def mnfp2_2_series(xml_data):
        root = ET.XML(xml_data)
        series_df = {}

        for item_number, item in enumerate(root):
            if item.tag in ["totalValueDailyLiquidAssets", "totalValueWeeklyLiquidAssets","percentageDailyLiquidAssets",
                            "percentageWeeklyLiquidAssets","netAssetValue"]:
                for subitem in item:
                    series_df[item.tag +"_"+subitem.tag] = subitem.text
            else:
                series_df[item.tag] = item.text
        series_df = pd.DataFrame(series_df, index = [0])
        return series_df

    series_df = mnfp2_2_series(seriesLevelInformation)

    # class level
    classLevelInformation_start = filing.find("<classLevelInfo>")
    classLevelInformation_end = filing.find("</classLevelInfo>")
    classLevelInformation = filing[
                            classLevelInformation_start:classLevelInformation_end + len("</classLevelInfo>")]
    classLevelInformation = classLevelInformation.replace("ns3", "")
    classLevelInformation = classLevelInformation.replace("nmfp2common", "")
    xml_data = classLevelInformation

    def mnfp2_2_class(xml_data):
        root = ET.XML(xml_data)
        class_df = {}

        for item_number, item in enumerate(root):
            if item.tag in ["netAssetShare","fridayWeek1","fridayWeek2","fridayWeek3","fridayWeek4","fridayWeek5",
                            "totalForTheMonthReported"]:
                for subitem in item:
                    class_df[item.tag+"_"+subitem.tag] = subitem.text
            else:
                class_df[item.tag] = item.text

        class_df = pd.DataFrame(class_df, index = [0])
        return class_df

    class_df =  mnfp2_2_class(classLevelInformation)

    # collateral and holdings
    scheduleOfPortfolioSecuritiesList_start = filing.find("<scheduleOfPortfolioSecuritiesInfo>")
    scheduleOfPortfolioSecuritiesList_end = filing.rfind("</scheduleOfPortfolioSecuritiesInfo>") + len("</scheduleOfPortfolioSecuritiesInfo>")
    scheduleOfPortfolioSecuritiesList = filing[scheduleOfPortfolioSecuritiesList_start:scheduleOfPortfolioSecuritiesList_end]
    scheduleOfPortfolioSecuritiesList = "<scheduleOfPortfolioSecuritiesList>" + scheduleOfPortfolioSecuritiesList  + "</scheduleOfPortfolioSecuritiesList>"
    scheduleOfPortfolioSecuritiesList = scheduleOfPortfolioSecuritiesList.replace("<date>", "")
    scheduleOfPortfolioSecuritiesList = scheduleOfPortfolioSecuritiesList.replace("</date>", "")
    scheduleOfPortfolioSecuritiesList = re.sub("> +", ">", scheduleOfPortfolioSecuritiesList)
    scheduleOfPortfolioSecuritiesList = re.sub(" +<", "<", scheduleOfPortfolioSecuritiesList)

    def mnfp2_2_holdingscollateral(xml_data):
        root = ET.XML(xml_data)  # element tree
        all_holdings_records = pd.DataFrame()
        all_collateral_records = pd.DataFrame()
        for issue_number, scheduleOfPortfolioSecurities in enumerate(root):
            holdings_record = {}


            for security_item in scheduleOfPortfolioSecurities:
                # grab all common fields (for repos, cp, cds...)
                holdings_record["issuer_number"] = issue_number + 1
                holdings_record[security_item.tag] = security_item.text

                if security_item.tag == "repurchaseAgreement":

                    for subsecurity_item in security_item:
                        if subsecurity_item.tag == "collateralIssuers":
                            collateral_record = {}
                            for subsubsecurity_item in subsecurity_item:
                                collateral_record["issuer_number"] = issue_number + 1
                                collateral_record[subsubsecurity_item.tag] = subsubsecurity_item.text
                            all_collateral_records = all_collateral_records.append(pd.DataFrame(collateral_record, index=[0]))
            all_holdings_records = all_holdings_records.append(pd.DataFrame(holdings_record, index = [0]))

        return all_holdings_records, all_collateral_records

    holdings, all_collateral = mnfp2_2_holdingscollateral(scheduleOfPortfolioSecuritiesList)

    return series_df, class_df, holdings, all_collateral

