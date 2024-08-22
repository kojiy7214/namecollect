import { CollectTargetRepository } from "./CollectTargetRepository.js"
import { NCSDao } from "../dao/NCSDao.js"

export let TargetSource = class{
    static instanceMap = {}

    static getInstance(target){
      /*  let instance = TargetSource.instanceMap[target]
      ? TargetSource.instanceMap[target]
      : (TargetSource.instanceMap[target] = new TargetSource(target))

      return instance*/

      return new TargetSource(target)
    }

    constructor(target){
        this.targetName = target

    }

    initIdMap(provider){}

    initColMap(provider){}

    loadIdMap(provider){}

    loadColMap(provider){}

    async getSyncCols(tenant){
        let retval = {}
        let sql = `
SELECT DISTINCT
	tname
FROM
	ncs.columnmap
WHERE
	tenant='${tenant}'
    and target = '${this.targetName}' and sync = 1        
`

        let result = await NCSDao.getInstance().query(sql)
        let target = CollectTargetRepository.getInstance(this.targetName, tenant)

        retval.id = target.alias2DB.id.default.replace(/(.*)::.*/, '$1')
        retval.param = []
        for ( let r of result ){
            let p ={}
            if ( target.alias2DB[r.tname] == undefined) {
                p.alias = r.tname
                p.db = await target.resolveUndefinedColumn(r.tname)

                if ( ! p.db ) continue
            }else{   
                p.alias = r.tname
                p.db =  target.alias2DB[r.tname].default.replaceAll(/::text/g, '')
            }
            retval.param.push(p)
        }
        return retval
    }

    /**
     * idmapの更新
     * @param {*} record 
     * @param {*} tenantId 
     */
    async updateIdMap(record, tenantId){
        //編集
        let  modified = async function(tid, listname, value, tenant){
            let sql = `
UPDATE ncs.idmap
SET 
    targetlist='{${listname.map(e => `"${e}"`).join(',')}}',
    targetstatus='MODIFIED',
    targetvalue='{"record":${JSON.stringify(value)}, "timestamp":${Date.now()}}'
WHERE
    tenant='${tenant}'
    and tid='${tid}'
            `
            await NCSDao.getInstance().execute(sql)
        }

        //削除
        let deleted = async function(tid, tenant){
            let sql = `
UPDATE ncs.idmap
SET 
    targetstatus='DELETED'
WHERE
    tenant='${tenant}'
    and tid='${tid}'
            `
            await NCSDao.getInstance().execute(sql)
        }

        //統合
        let integrated = async function(tid, toid, tenant){
            let sql = `
UPDATE ncs.idmap
SET 
    tid='${toid}'
WHERE
    tenant='${tenant}'
    and tid='${tid}'
            `
            await NCSDao.getInstance().execute(sql)
        }

        for ( let r of record){
            let action = r.action

            if ( action == 'MODIFIED' ){
                await modified(r.id, r.listname, r.param, tenantId)
            }else if ( action == 'DELETED' ){
                await deleted(r.id, tenantId)
            }else if ( action == 'INTEGRATED' ){
                await integrated(r.id, r.toid, tenantId)
            }
        }

    }
}