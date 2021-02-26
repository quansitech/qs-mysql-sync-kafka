const process = require('process');
const utils = require('./utils');
const kafka = require('kafka-node');
const AsyncLock = require('async-lock');
const nodemailer = require("nodemailer");

class SyncClient{

    constructor(opt, modules){
        const defaultOpt = {
            kafkaTopic: '',
            kafkaHost: '',
            adminEmail: '',
            nodemailerOpt: {
                host: '',
                port: 465,
                secure: true,
                user: '',
                password: ''
            }
        };

        this.options = Object.assign(defaultOpt, opt);
        this.initModules = [];
        this.modulesPack = {};

        modules.forEach(module => {
            const mod = module;
            if(typeof mod.init != 'undefined'){
                this.initModules.push(mod);
            }
        
            this.modulesPack[mod.name] = mod;
        });
    }

    async init(param){
        if(typeof param.module == 'undefined'){
            console.log('not found module parameter;');
            process.exit(0);
        }

        for(let i = 0; i < this.initModules.length; i++){
    
            if(typeof param.module == 'string' && (param.module == 'all' || param.module === this.initModules[i].name)){
                await this.initModules[i].init();
            }
    
            if(param.module && typeof param.module == 'object' && param.module.includes(this.initModules[i].name) === true ){
                await this.initModules[i].init();
            }
        }
    }

    startConsumer(param){
        let payloads, fromOffset;
        if(typeof param.offset != 'undefined'){
            payloads = [{ topic: this.options.kafkaTopic, offset: param.offset }];
            fromOffset = true;
        }
        else{
            payloads = [{ topic: this.options.kafkaTopic }];
            fromOffset = false;
        }

        const client = new kafka.KafkaClient({ kafkaHost: this.options.kafkaHost });
        const consumer = new kafka.Consumer(client, payloads, {
            autoCommit: false,
            fetchMaxWaitMs: 100,
            fetchMaxBytes: 1024 * 1024,
            fromOffset
        });

        let delayOperations = [];

        const lock = new AsyncLock();

        consumer.on('message', async message => {
            const esSyncData = JSON.parse(message.value);

            console.log(message);
            try{
                //如果有返回值 true，表示操作会延迟到该批次所有数据
                let delay = await this.modulesPack[esSyncData.data_type][esSyncData.op](esSyncData.data_key);
                if(delay && !delayOperations.includes(esSyncData.data_type + '-' + esSyncData.op)){
                    delayOperations.push(esSyncData.data_type + '-' + esSyncData.op);
                }

                if(message.offset == message.highWaterOffset - 1){
                    consumer.pause();

                    await lock.acquire('delay', async done => {
                        for(let n=0; n < delayOperations.length; n++){
                            const opTmp = delayOperations[n].split('-');
                            const dataType = opTmp[0];
                            const op = opTmp[1];
                            try{
                                await this.modulesPack[dataType][op](null, true);
                            }
                            catch(e){
                                done(e, null);
                            }
                            
                        }
                        delayOperations = [];
                        done(null, null);
                    });

                    consumer.commit((err, data) => {
                        consumer.resume();
                    })
                }
                
                
            }
            catch(e){
                if(this.options.adminEmail){
                    await this.sendMail(this.options.adminEmail, 'mysql->es同步出错', e.message);
                }
                
                consumer.close(function () {
                    console.log(e);
                    process.exit()
                });
            }
            
        });

        consumer.on('error', async function (err) {
            console.log('some thing err:', err);

            if(this.options.adminEmail){
                await this.sendMail(this.options.adminEmail, 'mysql->es同步出错', e.message);
            }

            consumer.close(function () {
                process.exit()
            });
        });

        process.on('SIGINT', function () {
            consumer.close(function () {
                process.exit()
            });
        });
    }

    async sendMail(to, subject, text){
        const opt = {
            host: this.options.nodemailerOpt.host,
            port: this.options.nodemailerOpt.port,
            secure: this.options.nodemailerOpt.host.secure, // true for 465, false for other ports
            auth: {
                user: this.options.nodemailerOpt.user, // generated ethereal user
                pass: this.options.nodemailerOpt.password, // generated ethereal password
            }
        };
        let transporter = nodemailer.createTransport(opt);
      
        // send mail with defined transport object
        let info = await transporter.sendMail({
          from: this.options.nodemailerOpt.user, // sender address
          to, // list of receivers
          subject, // Subject line
          text
        });
      
        console.log("Message sent: %s", info.messageId);
    }
}

module.exports = SyncClient;