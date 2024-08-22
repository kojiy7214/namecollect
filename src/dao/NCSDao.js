// import pg from 'pg'
import mssql from 'mssql'
import { Config } from '../conf.js'

// const { Pool } = pg

export let NCSDao = class{
    static instance

    static getInstance() {
      let instance = NCSDao.instance
        ? NCSDao.instance
        : (NCSDao.instance = new NCSDao())
  
        return instance
    }    

    constructor(){        
        //this.pool = new Pool(Config.getInstance().datasrc)
        this.pool = new mssql.ConnectionPool(Config.getInstance().datasrc);
        this.poolConnect = this.pool.connect();
    }

    async execute(q, transaction = true) {
        await this.poolConnect;
        let transactionObj = null;

        try {
            if (transaction) {
                transactionObj = new mssql.Transaction(this.pool);
                await transactionObj.begin();
            }

            const request = transactionObj 
            ? new mssql.Request(transactionObj) 
            : new mssql.Request();

            await request.query(q);

            if (transaction) {
                await transactionObj.commit();
            }
        } catch (e) {
            if (transaction && transactionObj) {
                await transactionObj.rollback();
            }
            throw new Error(`SQL exec failed: ${q}, error: ${e.message}`);
        }
    }

    async query(q, transaction = true) {
        await this.poolConnect;
        let transactionObj = null;
        let result;

        try {
            if (transaction) {
                transactionObj = new mssql.Transaction(this.pool);
                await transactionObj.begin();
            }

            const request = transactionObj
                ? new mssql.Request(transactionObj)
                : new mssql.Request();

            result = await request.query(q);
            if (transaction) {
                await transactionObj.commit();
            }
        } catch (e) {
            if (transaction && transactionObj) {
                await transactionObj.rollback();
            }
            throw new Error(`SQL query failed: ${q}, error: ${e.message}`);
        }

        return result.recordset;
    }
}