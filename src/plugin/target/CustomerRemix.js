// import pg from 'pg'
import mssql from 'mssql'
import fetch from 'node-fetch'
import {Config} from '../../conf.js'

// const { Pool } = pg

export let CustomerRemix = class {
    static instance
    static alias2DB = {
        table: {
            default: "customer",
            normalizer: null
        },
        id: {
            default: "CAST(customer.company_code AS VARCHAR(MAX)) AS id",
            normalizer: null,
            apicode: 318,
            type: "num"
        },
        businessCategory: {
            default: "CAST(customer.business_category AS VARCHAR(MAX)) AS businessCategory",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 301,
            type: "text"
        },
        companyName: {
            default: "CAST(customer.company_name AS VARCHAR(MAX)) AS companyName",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 301,
            type: "text"
        },
        companyKana: {
            default: "CAST(customer.company_kana AS VARCHAR(MAX)) AS companyKana",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 302,
            type: "text"
        },
        zipcodeC: {
            default: "CAST(customer.zipcode AS VARCHAR(MAX)) AS zipcodeC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 303,
            type: "text"
        },
        addressC: {
            default: "CAST(customer.address AS VARCHAR(MAX)) AS addressC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 304,
            type: "text"
        },
        phoneNoC: {
            default: "CAST(customer.tel_no AS VARCHAR(MAX)) AS phoneNoC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 305,
            type: "text"
        },
        telNo2: {
            default: "CAST(customer.tel_no_2 AS VARCHAR(MAX)) AS telNo2",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 306,
            type: "text"
        },
        faxNoC: {
            default: "CAST(customer.fax_no AS VARCHAR(MAX)) AS faxNoC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 307,
            type: "text"
        },
        urlC: {
            default: "CAST(customer.hp_url AS VARCHAR(MAX)) AS urlC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 308,
            type: "text"
        },
        stockExchange: {
            default: "CAST(customer.stock_exchange AS VARCHAR(MAX)) AS stockExchange",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 310,
            type: "num"
        },
        presidentName: {
            default: "CAST(customer.president_name AS VARCHAR(MAX)) AS presidentName",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 311,
            type: "text"
        },
        presidentKana: {
            default: "CAST(customer.president_kana AS VARCHAR(MAX)) AS presidentKana",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 312,
            type: "text"
        },
        establishDate: {
            default: "CAST(customer.establish_date AS VARCHAR(MAX)) AS establishDate",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 313,
            type: "date"
        },
        capital: {
            default: "CAST(customer.capital AS VARCHAR(MAX)) AS capital",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 315,
            type: "num"
        },
        employeeNum: {
            default: "CAST(customer.employee_num AS VARCHAR(MAX)) AS employeeNum",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 316,
            type: "num"
        },
        noteC: {
            default: "CAST(customer.note AS VARCHAR(MAX)) AS noteC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 309,
            type: "text"
        },
        validateFlagC: {
            default: "CAST(customer.validate_flag AS VARCHAR(MAX)) AS validateFlagC",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 335,
            type: "num"
        },
        agencyFlag: {
            default: "CAST(customer.establish_date AS VARCHAR(MAX)) AS agencyFlag",
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 338,
            type: "num"
        },
        industryKindCode: {
            default: `
            (
                SELECT user_message 
                FROM system_message_ja_jp 
                WHERE message_key = (
                    SELECT es.select_data 
                    FROM ext_select es 
                    WHERE es.extension_code = 340 
                      AND es.select_code = INDUSTRY_KIND_CODE
                )
            ) AS industryKindCode
            `,
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 340,
            type: "select"
        },
        customerLevel: {
            default: `
            (
                SELECT user_message 
                FROM system_message_ja_jp 
                WHERE message_key = (
                    SELECT cl.level_name 
                    FROM customer_level cl 
                    WHERE cl.customer_level = customer.customer_level
                )
            ) AS customerLevel
            `,
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 341,
            type: "sql",
            sql: `
            SELECT customer_level AS val
            FROM customer_level 
            LEFT JOIN system_message_ja_jp 
                ON customer_level.level_name = message_key 
            WHERE default_message = @val;
            `
        },
        customerRankCode: {
            default: `
            (
                SELECT user_message 
                FROM system_message_ja_jp 
                WHERE message_key = (
                    SELECT es.select_data 
                    FROM ext_select es 
                    WHERE es.extension_code = 339 
                      AND es.select_code = CUSTOMER_RANK_CODE
                )
            ) AS customerRankCode
            `,
            normalizer: ["[ー‐―－\\-\\s]", "ー"],
            apicode: 339,
            type: "select"
        },
        system_reg_date: {
            default: "customer.regist_date AS system_reg_date",
            normalizer: null
        },
        system_upd_date: {
            default: "customer.refix_date AS system_upd_date",
            normalizer: null
        },
        extension: {
            default: `
            SELECT 
                col_name AS ext_colname,
                CASE
                    WHEN ex_type = 0 THEN 'text'
                    WHEN ex_type = 1 THEN 'select'
                    WHEN ex_type = 2 THEN 'date'
                    WHEN ex_type = 3 THEN 'num'
                    WHEN ex_type = 4 THEN 'text'
                    WHEN ex_type = 5 THEN 'decimal'
                    WHEN ex_type = 6 THEN 'checkbox'
                    WHEN ex_type = 7 THEN 'text'
                    WHEN ex_type = 8 THEN 'text'
                    WHEN ex_type = 9 THEN 'text'
                    WHEN ex_type = 11 THEN 'date'
                    ELSE 'err'
                END AS ext_type
            FROM @tenant.extension_info 
            where
            ex_belong = 3
            AND CAST(extension_code AS NVARCHAR(MAX)) = @apicode;
            `
        }
    }    
  
    static getInstance(tenantId) {
      /*return CustomerRemix.instance
        ? CustomerRemix.instance
        : (CustomerRemix.instance = new CustomerRemix())*/

        return new CustomerRemix(tenantId)
    }

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
        return CustomerRemix.alias2DB
    }

    async query(q) {
        //QUERY内の置換対象文字列を置き換え
        for ( let key in CustomerRemix.alias2DB ){
            const rep_val = CustomerRemix.alias2DB[key].value.replace(/\s+AS\s+\w+$/i, '')
            let replaceTo = CustomerRemix.alias2DB[key].normalizer ?  
            this.generateNestedReplaceSQL(rep_val, CustomerRemix.alias2DB[key].normalizer[0], CustomerRemix.alias2DB[key].normalizer[1]) :
            rep_val;
            q = q.replaceAll("${" + key + "}", replaceTo)
            q = q.replaceAll("${" + key + "_default}", CustomerRemix.alias2DB[key].value)
        }

        //apply extensions
        let exts = [...q.matchAll(/\$\{(.+)_default\}/g)]

        for (let ext of exts){
            let sql = CustomerRemix.alias2DB['extension'].value
    
            let apicode = ext[1]
            sql = sql.replaceAll('@apicode', apicode).replaceAll('@tenant', this.tenantId)

            let result = await this.query(sql)

            if ( result == undefined) continue

            //modify alias2DB
            CustomerRemix.alias2DB[apicode] = {
                default: `CAST(customer.${result.ext_colname} AS VARCHAR(MAX)) AS "${ext[1]}"`,
                value: `CAST(customer.${result.ext_colname} AS VARCHAR(MAX)) AS "${ext[1]}"`,
                apicode: ext[1],
                type: result.ext_type
            }            

            //modify query
            q = q.replaceAll("${" + apicode + "_default}", CustomerRemix.alias2DB[apicode].value)
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

    // 正規表現からreplace文を生成する
    generateNestedReplaceSQL(targetString, regexPattern, replaceWith) {
        // 正規表現のパターンから、文字を抽出します。
        let pattern = regexPattern[0];
        let replacement = regexPattern[1];
    
        // エスケープされた文字を取り出します
        let charactersToReplace = [...new Set(pattern.replace(/[\[\]\\]/g, ''))];
        
        // 重複する文字は取り除きます
        let sql = targetString;
        
        // 文字ごとにREPLACE文を作成し、入れ子にします
        for (let char of charactersToReplace) {
            // 特殊文字をエスケープする
            let escapedChar = char.replace(/'/g, "''");
            sql = `REPLACE(${sql}, '${escapedChar}', '${replacement}')`;
        }
    
        return sql;
    }

    onMatch(query, param, colmap, result){
        query.param.companyCode = result.id
    }

    async onUnmatch(query, param, colmap, idmap){
        let body = {
            "objectName": "customer",
            "items": []
        }
        let retval = {}
        for ( let c in colmap){
            let p = param[c]
            let a = CustomerRemix.alias2DB[colmap[c]]
            if ( p && a && a.apicode){
                if ( a.type == 'select' ){
                    let url = Config.getInstance().appserver + this.tenantId + `/rest/v1/entities/selectitems?obj_name=customer&column_code=${a.apicode}`
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
                } else if(a.type == 'sql'){
                    let sql = a.sql.replaceAll('$\{val\}', p).replaceAll('$\{tenant\}', this.tenantId)
                    let sqlresult = await this.query(sql)
                  
                    if ( sqlresult == undefined){/*error*/}
                    let item = {}
                    item.column_code = a.apicode
                    item['num'] = parseInt(sqlresult.val)
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
            er.httpstatus = data?.code ? data.code : 500
            throw er
        }
        if ( data.code != undefined && data.code != 200 ){
            let er = new Error(`登録に失敗しました(メッセージ:${data?.messages[0]})`)
            er.httpstatus = data.code
            throw er
        }

        retval.id = data.primarykey
        query.param.companyCode = data.primarykey
        return retval
    }
}


// --- STATIC INITIALIZER ---
function compileAlias2DBValue(){
    const regexp = /\$\{(.*?)\}/g;

    let que = Object.keys(CustomerRemix.alias2DB)

    while (que.length != 0){
        let key = que.pop()
        let val = CustomerRemix.alias2DB[key].default
        let matches = Array.from(val.matchAll(regexp))

        if ( matches.length == 0 ){
            CustomerRemix.alias2DB[key].value = val
        }else{
            que.unshift(key)
            for ( let m of matches ){
                if ( CustomerRemix.alias2DB[m[1]].value ){
                    val = val.replaceAll(m[0], CustomerRemix.alias2DB[m[1]][CustomerRemix.alias2DB[key].refer])
                }
            }
            CustomerRemix.alias2DB[key].default = val
        }
    }
}

compileAlias2DBValue()