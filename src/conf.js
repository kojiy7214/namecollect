export let Config = class{
    static instance

    static getInstance(){
        let instance = Config.instance
        ? Config.instance
        : (Config.instance = new Config())
  
        return instance        
    }

    //接続情報
    constructor(){
        this.serverport = 3001,
        this.appserver = 'https://dev-solution.softbrain.co.jp/',
        this.apikey = '7d33251c-2f42-4299-b02b-063702706561',
        this.datasrc = {
            server: '172.26.1.4',
            user: 'sa',
            password: 'Softbrain1',
            database: 'ncs',
            max: 20,
            idleTimeoutMillis: 60000,
            connectionTimeoutMillis: 60000,
            options: {
                encrypt: false,
                trustServerCertificate: true,
            }
        }
        this.logs = {        
            appenders: { 
                access: { type: "file", filename: "./logs/access.log" }
                },
            categories: { 
                default: { appenders: ["access"], level: "debug" } 
            }
        }
        this.developerOnly = {
            secretKey: 'AXTEqyxMlZTbxfpsgoV53fsr8Zi4dSjy'
        }
    }
}