module.exports = {
    parseParam: () => {
        const param = {};
        process.argv.forEach(argv => {
          if(argv.startsWith("--")){
            const found = argv.match(/--(\w+?)=([a-zA-Z0-9,_-]+)/);
            if(found){
              param[found[1]] = found[2].split(',').length > 1 ? found[2].split(',') : found[2].split(',')[0];
            }
          }
        });
      
        return param;
    },
    chunkRun: async (chunkSize, arr, runFn) => {
      let temps;
      let index = 0;
      while((temps = arr.slice(index, index+chunkSize)) && temps.length>0){
          await runFn(temps);
          index += chunkSize;
      };
    }
}