import { NCSDao } from "../dao/NCSDao.js"
import {createPageSQL} from "../util/PageHelper.js"

export let IdMap = class{
    static instanceMap = {}

    static getInstance(provider, target, tenantId){
    /* let key = `${provider}+${target}`
       let instance = IdMap.instanceMap[key]
      ? IdMap.instanceMap[key]
      : (IdMap.instanceMap[key] = new IdMap(provider, target))

      return instance
        */
       
      return new IdMap(provider, target, tenantId)
    }

    constructor(provider, target, tenantId){
        this.provider = provider
        this.target = target
        this.tenantId = tenantId

        this.idmap = {}
    }

    async updateNullPid(pid, tid){
        let sql = `
UPDATE ncs.idmap
SET pid = '${pid}'
WHERE
    provider = '${this.provider}'
    and target = '${this.target}'
    and tenant = '${this.tenantId}'
    and tid = '${tid}'
    and pid IS null
        `
        await NCSDao.getInstance().execute(sql)
    }

    async save(){
        //UPSERT idmap
        //-- INSERT文とMERGE文を使用
let upsert = `MERGE ncs.idmap AS target
USING (VALUES
    ${function fn(){
        let retval = ''
        for (let pid in this.idmap){
            for (let tid in this.idmap[pid]){
                let e = this.idmap[pid][tid]
                retval = retval + `('${this.provider}', '${this.target}', ${e.pid ? `'${e.pid}'` : 'NULL'},  ${e.tid ? `'${e.tid}'` : 'NULL'}, ${e.targetvalue ? `'${e.targetvalue}'` : 'NULL'}, '${this.tenantId}', ${e.mergedvalue ? `'${e.mergedvalue}'` : 'NULL'}, ${e.option ? `'${e.option}'` : 'NULL'}),` + "\r\n"
            }
        }
        return retval.replace(/,\r\n$/, '');
    }.apply(this)}
) AS source (provider, target, pid, tid, targetvalue, tenant, mergedvalue, [option])
ON (target.provider = source.provider AND target.target = source.target AND target.pid = source.pid AND target.tenant = source.tenant)
WHEN MATCHED THEN
    UPDATE SET 
        target.tid = source.tid,
        target.targetvalue = COALESCE(source.targetvalue, target.targetvalue),
        target.mergedvalue = COALESCE(source.mergedvalue, target.mergedvalue),
        target.[option] = COALESCE(source.[option], target.[option])
WHEN NOT MATCHED THEN
    INSERT (provider, target, pid, tid, targetvalue, tenant, mergedvalue, [option])
    VALUES (source.provider, source.target, source.pid, source.tid, source.targetvalue, source.tenant, source.mergedvalue, source.[option]);`    

    await NCSDao.getInstance().execute(upsert)

    }

    add(pid, tid, targetlist, targetstatus, targetvalue, mergedvalue, option){
        targetvalue = targetvalue ? {record:targetvalue, timestamp:Date.now()} : null
        mergedvalue = mergedvalue ? {record:mergedvalue, timestamp:Date.now()} : null

        let val = {
            pid:pid, tid:tid,
            targetlist:targetlist, 
            targetstatus:targetstatus, 
            targetvalue:targetvalue, 
            mergedvalue:mergedvalue,
            option: option
        }


        this.idmap[pid] ={}
        this.idmap[pid][tid] = val
    }

    clear(){
        this.idpam = {}
    }

    async load(condition, page){
        let limitoffset = await createPageSQL(page)

        condition = condition ? condition : '1=1'
        let sql = `
SELECT 
    pid, tid, targetlist, targetstatus, targetvalue, mergedvalue, [option]
FROM
    ncs.idmap
WHERE
    provider = '${this.provider}'
    and target = '${this.target}'
    and tenant = '${this.tenantId}'
    and ${condition}
ORDER BY id
${limitoffset}
        `

        let result = await NCSDao.getInstance().query(sql)
        //this.clear()

        for ( let r of result ){
            let val = {
                pid:r.pid, 
                tid:r.tid, 
                targetlist: r.targetlist, 
                targetstatus:r.targetstatus, 
                targetvalue: r.targetvalue, 
                mergedvalue: r.mergedvalue,
                option: r.option
            }
            
            this.idmap[r.pid] = {}
            this.idmap[r.pid][r.tid] = val
        }
    }
}