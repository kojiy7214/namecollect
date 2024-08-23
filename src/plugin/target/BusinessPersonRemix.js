// import pg from 'pg'
import mssql from 'mssql'
import fetch from 'node-fetch'
import {Config} from '../../conf.js'
import { SQLBuilder } from '../../util/SQLBuilder.js'

import log4js from 'log4js'

// const { Pool } = pg

//log4js.configure(Config.getInstance().logs)

export let BusinessPersonRemix = class {
    static instance
    static alias2DB = {
        table: {
            default:"business_person LEFT OUTER JOIN @tenant.customer ON business_person.company_code = customer.company_code",
            normalizer: null},
        id:{
            default:"CAST(business_person.business_person_id AS VARCHAR(MAX)) as id",
            normalizer: null
        },
        name:{
            default:"CAST(business_person.person_name AS VARCHAR(MAX)) as name",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 903,
            type: "text"
        },
        nameKana:{
            default:"CAST(business_person.person_kana AS VARCHAR(MAX)) as kana",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 918,
            type: "text"
        }, 
        companyName:{
            default:"CAST(customer.company_name AS VARCHAR(MAX)) as companyName",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        }, 
        department:{
            default:"CAST(business_person.depart_name AS VARCHAR(MAX)) as department",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 904,
            type: "text"
        }, 
        post:{
            default:"CAST(business_person.post AS VARCHAR(MAX)) as post",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 905,
            type: "text"
        }, 
        email:{
            default:"CAST(business_person.email AS VARCHAR(MAX)) as email",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 914,
            type: "text"
        }, 
        mob_e_mail:{
            default:"CAST(business_person.e_mail AS VARCHAR(MAX)) as mob_e_mail",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 913,
            type: "text"
        },
        phoneNoB:{
            default:"CAST(business_person.tel_no AS VARCHAR(MAX)) as phoneNoB",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 909,
            type: "text"
        }, 
        mobilePhoneNo:{
            default:"CAST(business_person.mob_no AS VARCHAR(MAX)) as mobilePhoneNo",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 912,
            type: "text"
        }, 
        interphoneNo:{
            default:"CAST(business_person.interphone_no AS VARCHAR(MAX)) as interphoneNo",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 910,
            type: "text"
        }, 
        faxNoB:{
            default:"CAST(business_person.fax_no AS VARCHAR(MAX)) as faxNoB",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 911,
            type: "text"
        }, 
        zipcode: {
            default:"CAST(zipcode AS VARCHAR(MAX)) as zipcode",
            normalizer: ["[ー‐―－\\-〒\\s]", ""],
            apicode: 906,
            type: "text"
        },
        addressB: {
            default:"concat(${addressStreetB}, ${addressBuildingB}) as addressB",
            normalizer: ["[ー‐―－\\-〒\\s]", ""],
            refer: "default",
            apicode: 907,
            type: "text"
        },
        addressStreetB: {
            default:"CAST(business_person.address AS VARCHAR(MAX))",
            normalizer: ["[ー‐―－\\-〒\\s]", "-"],
            apicode: 907,
            type: "text"
        },
        addressBuildingB: {
            default:"CAST(business_person.street_number AS VARCHAR(MAX))",
            normalizer: ["[ー‐―－\\-\\s]", ""],
            apicode: 945,
            type: "text"
        },
        urlB:{
            default:"CAST(business_person.url AS VARCHAR(MAX)) as urlB",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 915,
            type: "text"
        }, 
        noteB:{
            default:"CAST(business_person.note AS VARCHAR(MAX)) as noteB",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 916,
            type: "text"
        },
        serviceFlag:{
            default:"CAST(business_person.service_flag AS VARCHAR(MAX)) as serviceFlag",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 925,
            type: "text"
        },
        validateFlagB:{
            default:"CAST(business_person.validate_flag AS VARCHAR(MAX)) as validateFlagB",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 927,
            type: "text"
        },
        postTypeCode:{
            default:"(select user_message from system_message_ja_jp where message_key = (select es.select_data from ext_select es where es.extension_code = 939 and es.select_code = POST_TYPE_CODE)) postTypeCode",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 939,
            type: "select"
        },
        departTypeCode:{
            default:"(select user_message from system_message_ja_jp where message_key = (select es.select_data from ext_select es where es.extension_code = 940 and es.select_code = DEPART_TYPE_CODE)) postTypeCode",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 940,
            type: "select"
        },
        companyCode:{
            default:"CAST(customer.company_code AS VARCHAR(MAX)) as companyCode",
            normalizer: null,
            apicode: 902,
            type: "num"
        },
        businessCategory:{
            default:"CAST(customer.business_category AS VARCHAR(MAX)) as businessCategory",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        companyKana:{
            default:"CAST(customer.company_kana AS VARCHAR(MAX)) as companyKana",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        zipcodeC:{
            default:"CAST(customer.zipcode AS VARCHAR(MAX)) as zipcodeC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        addressC:{
            default:"CAST(customer.address AS VARCHAR(MAX)) as addressC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        phoneNoC:{
            default:"CAST(customer.tel_no AS VARCHAR(MAX)) as phoneNoC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        telNo2:{
            default:"CAST(customer.tel_no_2 AS VARCHAR(MAX)) as telNo2",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        faxNoC:{
            default:"CAST(customer.fax_no AS VARCHAR(MAX)) as faxNoC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        urlC:{
            default:"CAST(customer.hp_url AS VARCHAR(MAX)) as urlC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        stockExchange:{
            default:"CAST(customer.stock_exchange AS VARCHAR(MAX)) as stockExchange",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        presidentName:{
            default:"CAST(customer.president_name AS VARCHAR(MAX)) as presidentName",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        presidentKana:{
            default:"CAST(customer.president_kana AS VARCHAR(MAX)) as presidentKana",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        establishDate:{
            default:"CAST(customer.establish_date AS VARCHAR(MAX)) as establishDate",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        capital:{
            default:"CAST(customer.capital AS VARCHAR(MAX)) as capital",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        employeeNum:{
            default:"CAST(customer.employee_num AS VARCHAR(MAX)) as employeeNum",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        noteC:{
            default:"CAST(customer.note AS VARCHAR(MAX)) as noteC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        validateFlagC:{
            default:"CAST(customer.validate_flag AS VARCHAR(MAX)) as validateFlagC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        agencyFlag:{
            default:"CAST(customer.establish_date AS VARCHAR(MAX)) as agencyFlag",
            normalizer: ["[ー‐―－\\-\\s]", "ー"]
        },
        industryKindCode: {
            default: `
            (SELECT user_message FROM system_message_ja_jp WHERE message_key = 
                (SELECT es.select_data FROM ext_select es WHERE es.extension_code = 340 AND es.select_code = INDUSTRY_KIND_CODE)
            ) AS industryKindCode`,
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 340,
            type: "select"
        },
        customerLevel: {
            default: `(SELECT user_message FROM system_message_ja_jp WHERE message_key = 
                        (SELECT cl.level_name FROM customer_level cl WHERE cl.customer_level = customer.customer_level)
                    ) AS customerLevel`,
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 341,
            type: "sql",
            sql: `SELECT customer_level AS val FROM customer_level LEFT JOIN system_message_ja_jp ON customer_level.level_name = message_key WHERE default_message = @val;`
        },
        customerRankCode: {
            default: `(SELECT user_message FROM system_message_ja_jp WHERE message_key = 
                        (SELECT es.select_data FROM ext_select es WHERE es.extension_code = 339 AND es.select_code = CUSTOMER_RANK_CODE)
            ) AS customerRankCode`,
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 339,
            type: "select"
        },
        system_reg_date:{
            default:"business_person.regist_date",
            normalizer: null
        },
        system_upd_date:{
            default:"business_person.refix_date",
            normalizer: null
        },
        extension:{
            default: `
select 
 col_name as ext_colname,
case
 when ex_type = 0 then 'text'
 when ex_type = 1 then 'select'
 when ex_type = 2 then 'date'
 when ex_type = 3 then 'num'
 when ex_type = 4 then 'text'
 when ex_type = 5 then 'decimal'
 when ex_type = 6 then 'checkbox'
 when ex_type = 7 then 'text'
 when ex_type = 8 then 'text'
 when ex_type = 9 then 'text'
 when ex_type = 11 then 'date'
 else 'err'
end ext_type
from @tenant.extension_info 
where
	ex_belong = 9
	and CAST(extension_code AS NVARCHAR(MAX)) = '@apicode';        
            `
        }
    }
  
    static getInstance(tenatId) {
      /*return BusinessPersonRemix.instance
        ? BusinessPersonRemix.instance
        : (BusinessPersonRemix.instance = new BusinessPersonRemix())*/
        return new BusinessPersonRemix(tenatId)
    }

    //DB接続情報
    constructor(tenantId) {
        this.type = 'sql'
        this.provider = 'mssql'
        this.tenantId = tenantId
        this.conf = {
            server: '172.26.1.4',
            user: 'sa',
            password: 'Softbrain1',
            database: tenantId,
            max: 20,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 2000,
            options: {
                encrypt: false,
                trustServerCertificate: true,
            }
        }
        
        this.pool = new mssql.ConnectionPool(this.conf);
        this.poolConnect = this.pool.connect();
    } 


    destructor(){
        this.pool.end()
    }

    get alias2DB(){
        return BusinessPersonRemix.alias2DB
    }

    async query(q) {
        //QUERY内の置換対象文字列を置き換え
        for ( let key in BusinessPersonRemix.alias2DB ){
            const rep_val = BusinessPersonRemix.alias2DB[key].value.replace(/\s+AS\s+\w+$/i, '')
            let sql = new SQLBuilder()
            let replaceTo = BusinessPersonRemix.alias2DB[key].normalizer ?  
            sql.generateNestedReplaceSQL(rep_val, BusinessPersonRemix.alias2DB[key].normalizer[0], BusinessPersonRemix.alias2DB[key].normalizer[1]) :
            rep_val;
            q = q.replaceAll("${" + key + "}", replaceTo)
            q = q.replaceAll("${" + key + "_default}", BusinessPersonRemix.alias2DB[key].value)
            q = q.replaceAll('@tenant', this.tenantId)
        }
        
        //apply extensions
        let exts = [...q.matchAll(/\$\{(.+)_default\}/g)]

        for (let ext of exts){
            let sql = BusinessPersonRemix.alias2DB['extension'].value
    
            let apicode = ext[1]
            sql = sql.replaceAll('@apicode', apicode).replaceAll('@tenant', this.tenantId)

            let result = await this.query(sql)

            if ( result == undefined)continue

            //modify alias2DB
            BusinessPersonRemix.alias2DB[apicode] ={
                default:`business_person.${result.ext_colname}::text as "${ext[1]}"`,
                value: `business_person.${result.ext_colname}::text as "${ext[1]}"`,
                apicode: ext[1],
                type: result.ext_type
            }

            //modify query
            q = q.replaceAll("${" + apicode + "_default}", BusinessPersonRemix.alias2DB[apicode].value)
        }

        //remove unmatched select columns
        q = q.replaceAll(/(,\$\{.+_default\})/g, '--$1')

        // データベース接続を待機
        await this.poolConnect;
        let transactionObj = null;
        let result;

        try {
            // トランザクションオブジェクトの作成
            transactionObj = new mssql.Transaction(this.pool);
            
            // トランザクションの開始
            await transactionObj.begin();

            // トランザクションが存在する場合はそのトランザクションを使い、存在しない場合は新しいリクエストを作成
            const request = transactionObj 
                ? new mssql.Request(transactionObj) 
                : new mssql.Request();

            // クエリの実行
            result = await request.query(q);
            
            // クエリの成功を確認し、トランザクションをコミット
            await transactionObj.commit();

        } catch (e) {
            if (transactionObj) {
                await transactionObj.rollback();
            }
            throw new Error(`SQL exec failed: ${q}, error: ${e.message}`);
        }

        return result.recordset;
    }

    onMatch(query, param, colmap, result){
        //param.id = result.id
    }

    async onMerge(query, param, colmap, result){
        let body = {
            "objectName": "person",
            "items": []
        }
        let retval = {}
        for ( let c in colmap){
            let p = result[c]
            let a = BusinessPersonRemix.alias2DB[c]
            if ( p && a && a.apicode){
                let item = {}
                item.column_code = a.apicode
                item[a.type] = p
                body.items.push(item)
                retval[c] = p
            }
        }
        let url = Config.getInstance().appserver + this.tenantId + '/rest/v1/entity'
        
        const response = await fetch(url, {
            method: 'put',
            body: JSON.stringify(body),
            headers: {'Content-Type': 'application/json', 'X-Auth-API-Token': Config.getInstance().apikey}
        });
        const data = await response.json()
        retval.id = data.primarykey
        return retval
    }


    async onUnmatch(query, param, colmap, idmap){
        let body = {
            "objectName": "person",
            "items": []
        }
        let retval = {}
        for ( let c in colmap){
            let p = param[c]
            let a = BusinessPersonRemix.alias2DB[colmap[c]]
            if ( p && a && a.apicode){
                if ( a.type == 'select' ){
                    let url = Config.getInstance().appserver + this.tenantId + `/rest/v1/entities/selectitems?obj_name=person&column_code=${a.apicode}`
                    const response = await fetch(url, {
                        method: 'get',
                        headers: {'Content-Type': 'application/json', 'X-Auth-API-Token': Config.getInstance().apikey}
                    });
                    const data = await response.json()
   
                    let selectItemCode
                    for ( let d of data.selectItems ){
                        if ( d.selectItemName == p ){
                            selectItemCode = d.selectItemCode
                            break
                        }
                    }
                    if ( selectItemCode == undefined ){
                        //error
                    }
                    let item = {}
                    item.column_code = a.apicode
                    item['num'] = selectItemCode
                    body.items.push(item)
                    retval[c] = p
                }else{
                    let item = {}
                    item.column_code = a.apicode
                    item[a.type] = p
                    body.items.push(item)
                    retval[c] = p
                }
            }
        }
        let url = Config.getInstance().appserver + this.tenantId + '/rest/v1/entity'
        
        const response = await fetch(url, {
            method: 'post',
            body: JSON.stringify(body),
            headers: {'Content-Type': 'application/json', 'X-Auth-API-Token': Config.getInstance().apikey}
        });
        let res 
        let data
        try{
            res = await response.text()
            data = JSON.parse(res)
        }catch(e){
            let er = new Error(`登録に失敗しました(メッセージ:${res})`)
            er.httpstatus = data.code
            throw er
        }
        if ( data.code != undefined && data.code != 200 ){
            let er = new Error(`登録に失敗しました(メッセージ:${data?.messages[0]})`)
            er.httpstatus = data.code
            throw er
        }
        retval.id = data.primarykey
        return retval
    }
}


// --- STATIC INITIALIZER ---
function compileAlias2DBValue(){
    const regexp = /\$\{(.*?)\}/g;

    let que = Object.keys(BusinessPersonRemix.alias2DB)

    while (que.length != 0){
        let key = que.pop()
        let val = BusinessPersonRemix.alias2DB[key].default
        let matches = Array.from(val.matchAll(regexp))

        if ( matches.length == 0 ){
            BusinessPersonRemix.alias2DB[key].value = val
        }else{
            que.unshift(key)
            for ( let m of matches ){
                if ( BusinessPersonRemix.alias2DB[m[1]].value ){
                    val = val.replaceAll(m[0], BusinessPersonRemix.alias2DB[m[1]][BusinessPersonRemix.alias2DB[key].refer])
                }
            }
            BusinessPersonRemix.alias2DB[key].default = val
        }
    }
}

compileAlias2DBValue()