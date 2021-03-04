const mysql = require('mysql');
const Es  = require('@elastic/elasticsearch');
const crypto = require('crypto');

let instances = [];

function optHash(opt){
    const hash = crypto.createHash('sha256');
    return hash.update(JSON.stringify(opt)).digest('hex');
}

class MysqlEsSync{

    constructor(opt){
        const defaultOpt = {
            mysqlConnectionLimit : 10,
            mysqlHost: '',
            mysqlPort: 3306,
            mysqlUser: '',
            mysqlPassword: '',
            mysqlDatabase: '',
            esHost: '',
            esPort: '',
            bulkFetchRow: 500
        };

        this.options = Object.assign(defaultOpt, opt);
        const mysqlOpt = {
            connectionLimit : this.options.mysqlConnectionLimit,
            host     : this.options.mysqlHost,
            port     : this.options.mysqlPort,
            user     : this.options.mysqlUser,
            password : this.options.mysqlPassword,
            database : this.options.mysqlDatabase
        };
        this.mysqlPool = mysql.createPool(mysqlOpt);

        this.esClient = new Es.Client({ node: `http://${this.options.esHost}:${this.options.esPort}`});
    }

    static getInstance(opt){
        const hashKey = optHash(opt);
        if(typeof instances[hashKey] == 'undefined'){
            instances[hashKey] = new MysqlEsSync(opt);
        }
        return instances[hashKey];
    }

    async mysqlQuery(sql, page, pageRow){
        const start = (page - 1) * pageRow;
        
        const p = new Promise((resolve, reject) => {
            this.mysqlPool.query(`${sql} limit ${start},${pageRow}`, function (error, results, fields) {
                if (error){
                    reject(error);
                }
                else {
                    resolve({ results, fields });
                }
            });
        })
    
        return p;
    }

    async mysqlQueryById(sql, pageRow, id, idColumn){
        const p = new Promise((resolve, reject) => {
    
            let querySql = sql;
            if(id && idColumn){
                querySql = `${idColumn} > '${id}' order by ${idColumn} asc`;
    
                if(sql.indexOf('where') !== -1){
                    querySql = `${sql} and ${querySql}`;
                }
                else{
                    querySql = `${sql} where ${querySql}`;
                }
            }
            querySql = `${querySql} limit ${pageRow}`;
    
            this.mysqlPool.query(querySql, function (error, results, fields) {
                if (error){
                    reject(error);
                }
                else {
                    resolve({ results, fields });
                }
            });
        });
    
        return p;
    }

    async esBulk(mysqlQueryRes, docKey, index, pipeline){
        const body = [];
    
        mysqlQueryRes.results.forEach((item) => {
    
            if(typeof item[docKey] == 'undefined'){
                throw new Error(`field not found: ${docKey}`)
            }
    
            body.push({ index: { _index: index, _id: item[docKey] } });
    
            const tmp = {};
            mysqlQueryRes.fields.forEach(field => {
                tmp[field.name] = item[field.name];
            });
            
            body.push(tmp);
        });
    
        const bulkRes = await this.esClient.bulk({
            pipeline,
            body
        });
    
        if(bulkRes.body.errors){
            bulkRes.body.items.forEach(item => {
                if(item.index.status >= 400){
                    console.log(item);
                }
            });
    
            throw new Error('bulk error');
        }
    }

    async deleteEsIndex(index){
        try{
            await this.esClient.indices.delete({
                index
            });

            console.log(`delete ${index}`);
        }
        catch(e){
            console.log(`${index} not exists`);
        }
    }

    async createEsIndex(index, mapping, settings){
        try{

            const body = {};
            if(typeof settings == 'undefined'){
                body.settings = {
                    number_of_shards: 3,
                    number_of_replicas: 2
                };
            }
            else{
                settings.number_of_shards = 3;
                settings.number_of_replicas = 2;

                body.settings = settings;
            }

            body.mappings = mapping;

            const res = await this.esClient.indices.create({
                index,
                body
            });

            console.log(`create index:${index}`);
        }
        catch(e){
            console.log(`fail to create index:${index}`);
            throw e;
        }
    }

    async deleteEsDoc(id, index){
        try{
            await this.esClient.delete({ id, index });

            console.log(`delete index:${index} doc:${id}`);
        }
        catch(e){
            console.log(`index:${index} doc:${id} not exists`);
        }
        
    }

    async bulkPutEsFromMysql(sql,docKey,index,pipeline){
        const pageRow = this.options.bulkFetchRow;
        let page = 1;

        let res = null;
        while((res = await this.mysqlQuery(sql, page, pageRow)) && res.results.length > 0){

            await this.esBulk(res, docKey, index, pipeline);

            page++;
        }

        console.log(`bulkPutEsFromMysql index:${index} finished!`);
    }

    async bulkPutEsFromMysqlById(sql, docKey, index, idColumn, pipeline){
        const pageRow = this.options.bulkFetchRow;

        let res = null;
        let id = null;
        while((res = await this.mysqlQueryById(sql, pageRow, id, idColumn)) && res.results.length > 0){

            await this.esBulk(res, docKey, index, pipeline);

            id = res.results.pop()[idColumn];
        }

        console.log(`bulkPutEsFromMysql index:${index} finished!`);
    }

    async deletePolicy(name){

        try{
            const res = await this.esClient.enrich.deletePolicy({
                name
            });

            console.log(`deleted policy: ${name}`);
        }
        catch(e){
            console.log(`${name} policy not exists`);
        }
        
    }

    async createPolicy(name, body){
        await this.esClient.enrich.putPolicy({
            name,
            body
        });

        console.log(`created policy: ${name}`);
    }

    async executePolicy(name, requestTimeout = 30000){
        try{
            await this.esClient.enrich.executePolicy({
                name
            }, {
                requestTimeout
            });
    
            console.log(`run policy: ${name}`);
        }
        catch(e){
            console.log('executePolicy:', e.meta.body);
            throw e;
        }
        
    }

    async createPipeline(id, body){
        try{
            await this.esClient.ingest.putPipeline({
                id,
                body
            });
    
            console.log(`create pipeline: ${id}`);
        }
        catch(e){
            console.log(e.meta.body.error);
            throw e;
        }
        
    }

    async deletePipeline(id){
        try{
            await this.esClient.ingest.deletePipeline({id});

            console.log(`deleted pipeline: ${id}`);
        }
        catch(e){
            console.log(`pipeline not exists: ${id}`);
        }
        
    }

    async esUpdateByQuery(index, pipeline, body){
        const res = await this.esClient.updateByQuery({
            index,
            pipeline,
            body
        });

        console.log(`esUpdateByQuery index:${index} finished`);
    }
}

module.exports = MysqlEsSync;