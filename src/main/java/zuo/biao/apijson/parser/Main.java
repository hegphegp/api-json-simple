package zuo.biao.apijson.parser;

import com.alibaba.fastjson.JSONObject;
import zuo.biao.apijson.parser.core.APIJSONProvider;
import zuo.biao.apijson.parser.core.SQLExplorer;
import zuo.biao.apijson.parser.core.SQLProviderException;
import zuo.biao.apijson.parser.core.StatementType;

public class Main {
    public static void main(String[] args) throws SQLProviderException {

//        String json = "{\n" +
//                "    \"[]\": { \n" +
//                "        \"page\": 0, \n" +
//                "        \"count\": 2,   \n" +
//                "        \"User\": { \n" +
//                "            \"sex\": 0 \n" +
//                "        },\n" +
//                "        \"Moment\": {\n" +
//                "            \"userId@\": \"/User/id\"\n" +
//                "        },\n" +
//                "        \"Comment[]\": { \n" +
//                "            \"page\": 0,\n" +
//                "            \"count\": 2,\n" +
//                "            \"Comment\": {\n" +
//                "                 \"momentId@\": \"[]/Moment/id\"\n" +
//                "             }\n" +
//                "        }\n" +
//                "    }\n" +
//                "}";

        String json = "{ "                                                   + "\r\n" +
                "    '[]': { "                                               + "\r\n" +
                "        'table1': { "                                       + "\r\n" +
                "            '@column': 'code,name', "                       + "\r\n" +
                "            'code': 'C86L', "                               + "\r\n" +
                "            'name': 'name' "                                + "\r\n" +
                "        }, "                                                + "\r\n" +
                "        'table2:t2': {"                                     + "\r\n" +
                "            '@column': 'id,docno'"                          + "\r\n" +
                "        },"                                                 + "\r\n" +
                "        'table3:t3': {"                                     + "\r\n" +
                "            '@column': 'id,docno'"                          + "\r\n" +
                "        }"                                                  + "\r\n" +
                "    },"                                                     + "\r\n" +
                "    'join': {"                                              + "\r\n" +
                "        '@innerJoin': ["                                    + "\r\n" +
                "            'table1.id=t2.id',"                             + "\r\n" +
                "            't2.id=t3.id'"                                  + "\r\n" +
                "        ],"                                                 + "\r\n" +
                "        '@leftOuterJoin': ["                                + "\r\n" +
                "            'table1.id=t2.id',"                             + "\r\n" +
                "            't2.id=t3.id'"                                  + "\r\n" +
                "        ]"                                                  + "\r\n" +
                "    }"                                                      + "\r\n" +
                "}";
        json = json.replaceAll("'", "\"");
        System.out.println(json);
        JSONObject req = JSONObject.parseObject(json);
        APIJSONProvider apijsonProvider = new APIJSONProvider(req);
        apijsonProvider.setStatementType(StatementType.SELECT);
//        apijsonProvider.setStatementType(StatementType.SELECT);
////		apijsonProvider.getTableBlackSet().add("Retail");
////		apijsonProvider.getTableWhiteSet().add("Retail");
////		apijsonProvider.getTableWhiteSet().add("StorE");
////		apijsonProvider.getColumnBlackList().add("retail.id");
////		apijsonProvider.getColumnWhiteList().add("retail.*");
////		apijsonProvider.getColumnWhiteList().add("retail.amt");
////		apijsonProvider.getColumnBlackList().add("retail.amt");
////		apijsonProvider.getColumnWhiteList().add("store.id");
        SQLExplorer builder = new SQLExplorer(apijsonProvider);
        System.out.println(builder.getSQL());


        System.out.println("\n\n\n\n");


//        json = "{\n" +
//                "    'c_store': {\n" +
//                "        '@column': 'code,name',\n" +
//                "        'code': 'C86L'\n" +
//                "    }\n" +
//                "}";
//        json = json.replaceAll("'", "\"");
//        System.out.println(json);
//        req = JSONObject.parseObject(json);
//        apijsonProvider = new APIJSONProvider(req);
//        apijsonProvider.setStatementType(StatementType.SELECT);
//        builder = new SQLExplorer(apijsonProvider);
//        System.out.println(builder.getSQL());
//
//
//        System.out.println("\n\n\n\n");
//
//
//        json = "{\n" +
//                "  \"Moment\":{\n" +
//                "  },\n" +
//                "  \"User\":{\n" +
//                "    \"id@\":\"/Moment/userId\"" +
//                "  }\n" +
//                "}";
//        System.out.println(json);
//        req = JSONObject.parseObject(json);
//        apijsonProvider = new APIJSONProvider(req);
//        apijsonProvider.setStatementType(StatementType.SELECT);
//        builder = new SQLExplorer(apijsonProvider);
//        System.out.println(builder.getSQL());
    }
}
