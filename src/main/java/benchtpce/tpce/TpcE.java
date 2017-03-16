package benchtpce.tpce;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * Created by carlosmorais on 20/11/2016.
 */
public class TpcE {

    public Map<String, List<Integer>> tablePositions;
    public Map<String, List<Integer>> tableKeys;

    public Map<String, Map<Integer, byte[]>> tablesColumns;

    public Map<Integer, byte[]> account_permissionColumns;
    public Map<Integer, byte[]> customerColumns;
    public Map<Integer, byte[]> customer_accountColumns;
    public Map<Integer, byte[]> customer_taxrateColumns;
    public Map<Integer, byte[]> holdingColumns;
    public Map<Integer, byte[]> holding_historyColumns;
    public Map<Integer, byte[]> holding_summaryColumns;
    public Map<Integer, byte[]> watch_itemColumns;
    public Map<Integer, byte[]> watch_listColumns;
    public Map<Integer, byte[]> brokerColumns;
    public Map<Integer, byte[]> cash_transactionColumns;
    public Map<Integer, byte[]> chargeColumns;
    public Map<Integer, byte[]> commission_rateColumns;
    public Map<Integer, byte[]> settlementColumns;
    public Map<Integer, byte[]> tradeColumns;
    public Map<Integer, byte[]> trade_historyColumns;
    public Map<Integer, byte[]> trade_requestColumns;
    public Map<Integer, byte[]> trade_typeColumns;
    public Map<Integer, byte[]> companyColumns;
    public Map<Integer, byte[]> company_competitorColumns;
    public Map<Integer, byte[]> daily_marketColumns;
    public Map<Integer, byte[]> exchangeColumns;
    public Map<Integer, byte[]> financialColumns;
    public Map<Integer, byte[]> industryColumns;
    public Map<Integer, byte[]> last_tradeColumns;
    public Map<Integer, byte[]> news_itemColumns;
    public Map<Integer, byte[]> news_xrefColumns;
    public Map<Integer, byte[]> sectorColumns;
    public Map<Integer, byte[]> securityColumns;
    public Map<Integer, byte[]> addressColumns;
    public Map<Integer, byte[]> status_typeColumns;
    public Map<Integer, byte[]> taxrateColumns;
    public Map<Integer, byte[]> zip_codeColumns;

    public static final String ACCOUNT_PERMISSION = "account_permission";
    public static final String CUSTOMER = "customer";
    public static final String CUSTOMER_ACCOUNT = "customer_account";
    public static final String CUSTOMER_TAXRATE = "customer_taxrate";
    public static final String HOLDING = "holding";
    public static final String HOLDING_HISTORY = "holding_history";
    public static final String HOLDING_SUMMARY = "holding_summary";
    public static final String WATCH_ITEM = "watch_item";
    public static final String WATCH_LIST = "watch_list";
    public static final String BROKER = "broker";
    public static final String CASH_TRANSACTION = "cash_transaction";
    public static final String CHARGE = "charge";
    public static final String COMMISSION_RATE = "commission_rate";
    public static final String SETTLEMENT = "settlement";
    public static final String TRADE = "trade";
    public static final String TRADE_HISTORY = "trade_history";
    public static final String TRADE_REQUEST = "trade_request";
    public static final String TRADE_TYPE = "trade_type";
    public static final String COMPANY = "company";
    public static final String COMPANY_COMPETITOR = "company_competitor";
    public static final String DAILY_MARKET = "daily_market";
    public static final String EXCHANGE = "exchange";
    public static final String FINANCIAL = "financial";
    public static final String INDUSTRY = "industry";
    public static final String LAST_TRADE = "last_trade";
    public static final String NEWS_ITEM = "news_item";
    public static final String NEWS_XREF = "news_xref";
    public static final String SECTOR = "sector";
    public static final String SECURITY = "security";
    public static final String ADDRESS = "address";
    public static final String STATUS_TYPE = "status_type";
    public static final String TAXRATE = "taxrate";
    public static final String ZIP_CODE = "zip_code";

    //Table family
    public static byte[] family = Bytes.toBytes("table");


    public TpcE() {
        tablePositions = new HashMap<>();
        tableKeys = new HashMap<>();
        tablesColumns = new HashMap<>();

        account_permissionColumns = new TreeMap<>();
        customerColumns = new TreeMap<>();
        customer_accountColumns = new TreeMap<>();
        customer_taxrateColumns = new TreeMap<>();
        holdingColumns = new TreeMap<>();
        holding_historyColumns = new TreeMap<>();
        holding_summaryColumns = new TreeMap<>();
        watch_itemColumns = new TreeMap<>();
        watch_listColumns = new TreeMap<>();
        brokerColumns = new TreeMap<>();
        cash_transactionColumns = new TreeMap<>();
        chargeColumns = new TreeMap<>();
        commission_rateColumns = new TreeMap<>();
        settlementColumns = new TreeMap<>();
        tradeColumns = new TreeMap<>();
        trade_historyColumns = new TreeMap<>();
        trade_requestColumns = new TreeMap<>();
        trade_typeColumns = new TreeMap<>();
        companyColumns = new TreeMap<>();
        company_competitorColumns = new TreeMap<>();
        daily_marketColumns = new TreeMap<>();
        exchangeColumns = new TreeMap<>();
        financialColumns = new TreeMap<>();
        industryColumns = new TreeMap<>();
        last_tradeColumns = new TreeMap<>();
        news_itemColumns = new TreeMap<>();
        news_xrefColumns = new TreeMap<>();
        sectorColumns = new TreeMap<>();
        securityColumns = new TreeMap<>();
        addressColumns = new TreeMap<>();
        status_typeColumns = new TreeMap<>();
        taxrateColumns = new TreeMap<>();
        zip_codeColumns = new TreeMap<>();

        //Table account_permission
        tablePositions.put(ACCOUNT_PERMISSION, Arrays.asList(2,4,5));
        tableKeys.put(ACCOUNT_PERMISSION, Arrays.asList(1,3));
        account_permissionColumns.put(2, Bytes.toBytes("ap_acl"));
        account_permissionColumns.put(4, Bytes.toBytes("ap_l_name"));
        account_permissionColumns.put(5, Bytes.toBytes("ap_f_name"));
        tablesColumns.put(ACCOUNT_PERMISSION, account_permissionColumns);

        //Table costumer
        tablePositions.put(CUSTOMER, Arrays.asList(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24));
        tableKeys.put(CUSTOMER, Arrays.asList(1));
        customerColumns.put(2, Bytes.toBytes("c_tax_id"));
        customerColumns.put(3, Bytes.toBytes("c_st_id"));
        customerColumns.put(4, Bytes.toBytes("c_l_name"));
        customerColumns.put(5, Bytes.toBytes("c_f_name"));
        customerColumns.put(6, Bytes.toBytes("c_m_name"));
        customerColumns.put(7, Bytes.toBytes("c_gndr"));
        customerColumns.put(8, Bytes.toBytes("c_tier"));
        customerColumns.put(9, Bytes.toBytes("c_dob"));
        customerColumns.put(10, Bytes.toBytes("c_ad_id"));
        customerColumns.put(11, Bytes.toBytes("c_ctry_1"));
        customerColumns.put(12, Bytes.toBytes("c_area_1"));
        customerColumns.put(13, Bytes.toBytes("c_local_1"));
        customerColumns.put(14, Bytes.toBytes("c_ext_1"));
        customerColumns.put(15, Bytes.toBytes("c_ctry_2"));
        customerColumns.put(16, Bytes.toBytes("c_area_2"));
        customerColumns.put(17, Bytes.toBytes("c_local_2"));
        customerColumns.put(18, Bytes.toBytes("c_ext_2"));
        customerColumns.put(19, Bytes.toBytes("c_ctry_3"));
        customerColumns.put(20, Bytes.toBytes("c_area_3"));
        customerColumns.put(21, Bytes.toBytes("c_local_3"));
        customerColumns.put(22, Bytes.toBytes("c_ext_3"));
        customerColumns.put(23, Bytes.toBytes("c_email_1"));
        customerColumns.put(24, Bytes.toBytes("c_email_2"));
        tablesColumns.put(CUSTOMER, customerColumns);


        //Table customer_account
        tablePositions.put(CUSTOMER_ACCOUNT, Arrays.asList(2,3,4,5,6));
        tableKeys.put(CUSTOMER_ACCOUNT, Arrays.asList(1));
        customer_accountColumns.put(2, Bytes.toBytes("ca_b_id"));
        customer_accountColumns.put(3, Bytes.toBytes("ca_c_id"));
        customer_accountColumns.put(4, Bytes.toBytes("ca_name"));
        customer_accountColumns.put(5, Bytes.toBytes("ca_tax_st"));
        customer_accountColumns.put(6, Bytes.toBytes("ca_bal"));
        tablesColumns.put(CUSTOMER_ACCOUNT, customer_accountColumns);


        //Table customer_taxrate
        tablePositions.put(CUSTOMER_TAXRATE, new ArrayList<>());
        tableKeys.put(CUSTOMER_TAXRATE, Arrays.asList(1,2));
        //all columns are the rowkey
        tablesColumns.put(CUSTOMER_TAXRATE, customer_taxrateColumns);


        //Table holding
        tablePositions.put(HOLDING, Arrays.asList(2,3,4,5,6));
        tableKeys.put(HOLDING, Arrays.asList(1));
        holdingColumns.put(2, Bytes.toBytes("h_ca_id"));
        holdingColumns.put(3, Bytes.toBytes("h_s_symb"));
        holdingColumns.put(4, Bytes.toBytes("h_dts"));
        holdingColumns.put(5, Bytes.toBytes("h_price"));
        holdingColumns.put(6, Bytes.toBytes("h_qty"));
        tablesColumns.put(HOLDING, holdingColumns);


        //Table holding_history
        tablePositions.put(HOLDING_HISTORY, Arrays.asList(3,4));
        tableKeys.put(HOLDING_HISTORY, Arrays.asList(1,2));
        holding_historyColumns.put(3, Bytes.toBytes("hh_before_qty"));
        holding_historyColumns.put(4, Bytes.toBytes("hh_after_qty"));
        tablesColumns.put(HOLDING_HISTORY, holding_historyColumns);


        //Table holding_summary
        tablePositions.put(HOLDING_SUMMARY, Arrays.asList(3));
        tableKeys.put(HOLDING_SUMMARY, Arrays.asList(1,2));
        holding_summaryColumns.put(3, Bytes.toBytes("hs_qty"));
        tablesColumns.put(HOLDING_SUMMARY, holding_summaryColumns);


        //Table watch_item
        tablePositions.put(WATCH_ITEM, new ArrayList<>());
        tableKeys.put(WATCH_ITEM, Arrays.asList(1,2));
        //all columns are the rowkey
        tablesColumns.put(WATCH_ITEM, watch_itemColumns);


        //Table watch_list
        tablePositions.put(WATCH_LIST, Arrays.asList(2));
        tableKeys.put(WATCH_LIST, Arrays.asList(1));
        watch_listColumns.put(2, Bytes.toBytes("wl_c_id"));
        tablesColumns.put(WATCH_LIST, watch_listColumns);

        //Table broker
        tablePositions.put(BROKER, Arrays.asList(2,3,4,5));
        tableKeys.put(BROKER, Arrays.asList(1));
        brokerColumns.put(2, Bytes.toBytes("b_st_id"));
        brokerColumns.put(3, Bytes.toBytes("b_name"));
        brokerColumns.put(4, Bytes.toBytes("b_num_trades"));
        brokerColumns.put(5, Bytes.toBytes("b_comm_total"));
        tablesColumns.put(BROKER, brokerColumns);


        //Table cash_transaction
        tablePositions.put(CASH_TRANSACTION, Arrays.asList(2,3,4));
        tableKeys.put(CASH_TRANSACTION, Arrays.asList(1));
        cash_transactionColumns.put(2, Bytes.toBytes("ct_dts"));
        cash_transactionColumns.put(3, Bytes.toBytes("ct_amt"));
        cash_transactionColumns.put(4, Bytes.toBytes("ct_name"));
        tablesColumns.put(CASH_TRANSACTION, cash_transactionColumns);


        //Table charge
        tablePositions.put(CHARGE, Arrays.asList(3));
        tableKeys.put(CHARGE, Arrays.asList(1,2));
        chargeColumns.put(3, Bytes.toBytes("ch_chrg"));
        tablesColumns.put(CHARGE, chargeColumns);


        //Table commission_rate
        tablePositions.put(COMMISSION_RATE, Arrays.asList(5,6));
        tableKeys.put(COMMISSION_RATE, Arrays.asList(1,2,3,4));
        commission_rateColumns.put(5, Bytes.toBytes(""));
        commission_rateColumns.put(6, Bytes.toBytes(""));
        tablesColumns.put(COMMISSION_RATE, commission_rateColumns);


        //Table settlement
        tablePositions.put(SETTLEMENT, Arrays.asList(2,3,4));
        tableKeys.put(SETTLEMENT, Arrays.asList(1));
        settlementColumns.put(2, Bytes.toBytes("se_cash_type"));
        settlementColumns.put(3, Bytes.toBytes("se_cash_due_date"));
        settlementColumns.put(4, Bytes.toBytes("se_amt"));
        tablesColumns.put(SETTLEMENT, settlementColumns);


        //Table trade
        tablePositions.put(TRADE, Arrays.asList(2,3,4,5,6,7,8,9,10,11,12,13,14,15));
        tableKeys.put(TRADE, Arrays.asList(1));
        tradeColumns.put(2, Bytes.toBytes("t_dts"));
        tradeColumns.put(3, Bytes.toBytes("t_st_id"));
        tradeColumns.put(4, Bytes.toBytes("t_tt_id"));
        tradeColumns.put(5, Bytes.toBytes("t_is_cash"));
        tradeColumns.put(6, Bytes.toBytes("t_s_symb"));
        tradeColumns.put(7, Bytes.toBytes("t_qty"));
        tradeColumns.put(8, Bytes.toBytes("t_bid_price"));
        tradeColumns.put(9, Bytes.toBytes("t_ca_id"));
        tradeColumns.put(10, Bytes.toBytes("t_exec_name"));
        tradeColumns.put(11, Bytes.toBytes("t_trade_price"));
        tradeColumns.put(12, Bytes.toBytes("t_chrg"));
        tradeColumns.put(13, Bytes.toBytes("t_comm"));
        tradeColumns.put(14, Bytes.toBytes("t_tax"));
        tradeColumns.put(15, Bytes.toBytes("t_lifo"));
        tablesColumns.put(TRADE, tradeColumns);


        //Table trade_history
        tablePositions.put(TRADE_HISTORY, Arrays.asList(2));
        tableKeys.put(TRADE_HISTORY, Arrays.asList(1,3));
        trade_historyColumns.put(2, Bytes.toBytes("th_dts"));
        tablesColumns.put(TRADE_HISTORY, trade_historyColumns);


        //Table trade_request
        tablePositions.put(TRADE_REQUEST, Arrays.asList(2,3,4,5,6));
        tableKeys.put(TRADE_REQUEST, Arrays.asList(1));
        trade_requestColumns.put(2, Bytes.toBytes("tr_tt_id"));
        trade_requestColumns.put(3, Bytes.toBytes("tr_s_symb"));
        trade_requestColumns.put(4, Bytes.toBytes("tr_qty"));
        trade_requestColumns.put(5, Bytes.toBytes("tr_bid_price"));
        trade_requestColumns.put(6, Bytes.toBytes("tr_b_id"));
        tablesColumns.put(TRADE_REQUEST, trade_requestColumns);

        //Table trade_type
        tablePositions.put(TRADE_TYPE, Arrays.asList(2,3,4));
        tableKeys.put(TRADE_TYPE, Arrays.asList(1));
        trade_typeColumns.put(2, Bytes.toBytes("tt_name"));
        trade_typeColumns.put(3, Bytes.toBytes("tt_is_sell"));
        trade_typeColumns.put(4, Bytes.toBytes("tt_is_mrkt"));
        tablesColumns.put(TRADE_TYPE, trade_typeColumns);


        //Table company
        tablePositions.put(COMPANY, Arrays.asList(2,3,4,5,6,7,8));
        tableKeys.put(COMPANY, Arrays.asList(1));
        companyColumns.put(2, Bytes.toBytes("co_st_id"));
        companyColumns.put(3, Bytes.toBytes("co_name"));
        companyColumns.put(4, Bytes.toBytes("co_in_id"));
        companyColumns.put(5, Bytes.toBytes("co_sp_rate"));
        companyColumns.put(6, Bytes.toBytes("co_ceo"));
        companyColumns.put(7, Bytes.toBytes("co_ad_id"));
        companyColumns.put(8, Bytes.toBytes("co_desc"));
        tablesColumns.put(COMPANY, companyColumns);


        //Table company_competitor
        tablePositions.put(COMPANY_COMPETITOR, new ArrayList<>());
        tableKeys.put(COMPANY_COMPETITOR, Arrays.asList(1,2,3));
        //all columns are the rowkey
        tablesColumns.put(COMPANY_COMPETITOR, company_competitorColumns);


        //Table daily_market
        tablePositions.put(DAILY_MARKET, Arrays.asList(3,4,5,6));
        tableKeys.put(DAILY_MARKET, Arrays.asList(1,2));
        daily_marketColumns.put(3, Bytes.toBytes("dm_close"));
        daily_marketColumns.put(4, Bytes.toBytes("dm_high"));
        daily_marketColumns.put(5, Bytes.toBytes("dm_low"));
        daily_marketColumns.put(6, Bytes.toBytes("dm_vol"));
        tablesColumns.put(DAILY_MARKET, daily_marketColumns);


        //Table exchange
        tablePositions.put(EXCHANGE, Arrays.asList(2,3,4,5,6,7));
        tableKeys.put(EXCHANGE, Arrays.asList(1));
        exchangeColumns.put(2, Bytes.toBytes("ex_name"));
        exchangeColumns.put(3, Bytes.toBytes("ex_num_symb"));
        exchangeColumns.put(4, Bytes.toBytes("ex_open"));
        exchangeColumns.put(5, Bytes.toBytes("ex_close"));
        exchangeColumns.put(6, Bytes.toBytes("ex_desc"));
        exchangeColumns.put(7, Bytes.toBytes("ex_ad_id"));
        tablesColumns.put(EXCHANGE, exchangeColumns);


        //Table financial
        tablePositions.put(FINANCIAL, Arrays.asList(4,5,6,7,8,9,10,11,12,13,14));
        tableKeys.put(FINANCIAL, Arrays.asList(1,2,3));
        financialColumns.put(4, Bytes.toBytes("fi_qtr_start_date"));
        financialColumns.put(5, Bytes.toBytes("fi_revenue"));
        financialColumns.put(6, Bytes.toBytes("fi_net_earn"));
        financialColumns.put(7, Bytes.toBytes("fi_basic_eps"));
        financialColumns.put(8, Bytes.toBytes("fi_dilut_eps"));
        financialColumns.put(9, Bytes.toBytes("fi_margin"));
        financialColumns.put(10, Bytes.toBytes("fi_inventory"));
        financialColumns.put(11, Bytes.toBytes("fi_assets"));
        financialColumns.put(12, Bytes.toBytes("fi_liability"));
        financialColumns.put(13, Bytes.toBytes("fi_out_basic"));
        financialColumns.put(14, Bytes.toBytes("fi_out_dilut"));
        tablesColumns.put(FINANCIAL, financialColumns);


        //Table industry
        tablePositions.put(INDUSTRY, Arrays.asList(2,3));
        tableKeys.put(INDUSTRY, Arrays.asList(1));
        industryColumns.put(2, Bytes.toBytes("in_name"));
        industryColumns.put(3, Bytes.toBytes("in_sc_id"));
        tablesColumns.put(INDUSTRY, industryColumns);


        //Table last_trade
        tablePositions.put(LAST_TRADE, Arrays.asList(2,3,4,5));
        tableKeys.put(LAST_TRADE, Arrays.asList(1));
        last_tradeColumns.put(2, Bytes.toBytes("lt_dts"));
        last_tradeColumns.put(3, Bytes.toBytes("lt_price"));
        last_tradeColumns.put(4, Bytes.toBytes("lt_open_price"));
        last_tradeColumns.put(5, Bytes.toBytes("lt_vol"));
        tablesColumns.put(LAST_TRADE, last_tradeColumns);


        //Table news_item
        tablePositions.put(NEWS_ITEM, Arrays.asList(2,3,4,5,6,7));
        tableKeys.put(NEWS_ITEM, Arrays.asList(1));
        news_itemColumns.put(2, Bytes.toBytes("ni_headline"));
        news_itemColumns.put(3, Bytes.toBytes("ni_summary"));
        news_itemColumns.put(4, Bytes.toBytes("ni_item"));
        news_itemColumns.put(5, Bytes.toBytes("ni_dts"));
        news_itemColumns.put(6, Bytes.toBytes("ni_source"));
        news_itemColumns.put(7, Bytes.toBytes("ni_author"));
        tablesColumns.put(NEWS_ITEM, news_itemColumns);


        //Table news_xref
        tablePositions.put(NEWS_XREF, new ArrayList<>());
        tableKeys.put(NEWS_XREF, Arrays.asList(1,2));
        //all columns are the rowkey
        tablesColumns.put(NEWS_XREF, news_xrefColumns);


        //Table sector
        tablePositions.put(SECTOR, Arrays.asList(2));
        tableKeys.put(SECTOR, Arrays.asList(1));
        sectorColumns.put(2, Bytes.toBytes("sc_name"));
        tablesColumns.put(SECTOR, sectorColumns);


        //Table security
        tablePositions.put(SECURITY, Arrays.asList(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16));
        tableKeys.put(SECURITY, Arrays.asList(1));
        securityColumns.put(2, Bytes.toBytes("s_issue"));
        securityColumns.put(3, Bytes.toBytes("s_st_id"));
        securityColumns.put(4, Bytes.toBytes("s_name"));
        securityColumns.put(5, Bytes.toBytes("s_ex_id"));
        securityColumns.put(6, Bytes.toBytes("s_co_id"));
        securityColumns.put(7, Bytes.toBytes("s_num_out"));
        securityColumns.put(8, Bytes.toBytes("s_start_date"));
        securityColumns.put(9, Bytes.toBytes("s_exch_date"));
        securityColumns.put(10, Bytes.toBytes("s_pe"));
        securityColumns.put(11, Bytes.toBytes("s_52wk_high"));
        securityColumns.put(12, Bytes.toBytes("s_52wk_high_date"));
        securityColumns.put(13, Bytes.toBytes("s_52wk_low"));
        securityColumns.put(14, Bytes.toBytes("s_52wk_low_date"));
        securityColumns.put(15, Bytes.toBytes("s_dividend"));
        securityColumns.put(16, Bytes.toBytes("s_yield"));
        tablesColumns.put(SECURITY, securityColumns);


        //Table address
        tablePositions.put(ADDRESS, Arrays.asList(2,3,4,5));
        tableKeys.put(ADDRESS, Arrays.asList(1));
        addressColumns.put(2, Bytes.toBytes("ad_line1"));
        addressColumns.put(3, Bytes.toBytes("ad_line2"));
        addressColumns.put(4, Bytes.toBytes("ad_zc_code"));
        addressColumns.put(5, Bytes.toBytes("ad_ctry"));
        tablesColumns.put(ADDRESS, addressColumns);


        //Table status_type
        tablePositions.put(STATUS_TYPE, Arrays.asList(2));
        tableKeys.put(STATUS_TYPE, Arrays.asList(1));
        status_typeColumns.put(2, Bytes.toBytes("st_name"));
        tablesColumns.put(STATUS_TYPE, status_typeColumns);


        //Table taxrate
        tablePositions.put(TAXRATE, Arrays.asList(2,3));
        tableKeys.put(TAXRATE, Arrays.asList(1));
        taxrateColumns.put(2, Bytes.toBytes("tx_name"));
        taxrateColumns.put(3, Bytes.toBytes("tx_rate"));
        tablesColumns.put(TAXRATE, taxrateColumns);


        //Table zip_code
        tablePositions.put(ZIP_CODE, Arrays.asList(2,3));
        tableKeys.put(ZIP_CODE, Arrays.asList(1));
        zip_codeColumns.put(2, Bytes.toBytes("zc_town"));
        zip_codeColumns.put(3, Bytes.toBytes("zc_div"));
        tablesColumns.put(ZIP_CODE, zip_codeColumns);
    }
}
